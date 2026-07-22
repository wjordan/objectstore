package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// S3Config is the constructor input for OpenS3. Only Bucket is
// required; everything else has reasonable defaults.
type S3Config struct {
	Bucket string // required.

	// Prefix is prepended to every key. A trailing "/" is normalized.
	Prefix string

	// Region for the bucket. If empty, the SDK reads AWS_REGION /
	// AWS_DEFAULT_REGION from the environment / shared config.
	Region string

	// EndpointURL overrides the default S3 endpoint. Set this for
	// S3-compatible services (R2, B2, MinIO, GCS-with-S3-API). When
	// non-empty, UsePathStyle defaults to true unless explicitly false.
	EndpointURL string

	// UsePathStyle forces path-style addressing
	// (https://endpoint/bucket/key) instead of virtual-host style.
	// Required for MinIO and most non-AWS S3-compatible services.
	UsePathStyle bool

	// AccessKey/SecretKey/SessionToken override the SDK credential
	// chain. Leave empty to use the default chain (env, shared
	// config, IAM role, IMDSv2).
	AccessKey    string
	SecretKey    string
	SessionToken string

	// HTTPClient overrides the SDK's default HTTP client. Tests pass
	// custom clients; production almost never needs this.
	HTTPClient *http.Client

	// ForceIPv4 dials the endpoint over IPv4 only. Some S3-compatible
	// anycast endpoints (Tigris) route IPv6 to a far PoP while IPv4 lands
	// on a near one, so forcing IPv4 can roughly halve per-GET latency.
	// Ignored when HTTPClient is set.
	ForceIPv4 bool

	// Clients is the number of striped transports (distinct TCP
	// connections under HTTP/2) requests are spread across, with
	// rate-based eviction of path-pinned connections — see clientSet.
	// 0 = default (4). Ignored when HTTPClient is set.
	Clients int

	// MaxAttempts caps SDK-level retries. 0 = SDK default (3).
	MaxAttempts int
}

// S3 implements Bucket against an S3 or S3-compatible bucket via
// aws-sdk-go-v2/service/s3.
type S3 struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	prefix   string // empty or ends with "/"
}

// OpenS3 constructs an S3 from cfg. It does not perform a network
// round-trip; the first call surfaces connectivity issues.
func OpenS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("objectstore/s3: Bucket required")
	}
	prefix := cfg.Prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	loadOpts := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.Region))
	}
	if cfg.HTTPClient != nil {
		loadOpts = append(loadOpts, config.WithHTTPClient(cfg.HTTPClient))
	} else {
		loadOpts = append(loadOpts, config.WithHTTPClient(&http.Client{
			Transport: newClientSet(cfg.Clients, cfg.ForceIPv4),
		}))
	}
	if cfg.AccessKey != "" || cfg.SecretKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		))
	}
	if cfg.MaxAttempts > 0 {
		loadOpts = append(loadOpts, config.WithRetryer(func() awsv2.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = cfg.MaxAttempts
			})
		}))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("objectstore/s3: load config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// S3-compatible stores often omit optional checksum headers on
		// GetObject; keep validation when present, but don't warn per read.
		o.DisableLogOutputChecksumValidationSkipped = true
		if cfg.EndpointURL != "" {
			o.BaseEndpoint = awsv2.String(cfg.EndpointURL)
			o.UsePathStyle = true
		}
		if cfg.UsePathStyle {
			o.UsePathStyle = true
		}
	})
	return &S3{
		client:   client,
		uploader: manager.NewUploader(client),
		bucket:   cfg.Bucket,
		prefix:   prefix,
	}, nil
}

func (a *S3) keyOf(k string) string { return a.prefix + k }

// applyIfMatch maps objectstore's ifMatch convention onto an S3 PutObjectInput.
//   - nil:    no precondition.
//   - &"":    IfNoneMatch=*  (must not exist)
//   - &etag:  IfMatch=etag   (must match)
func applyIfMatch(in *s3.PutObjectInput, ifMatch *string) {
	if ifMatch == nil {
		return
	}
	if *ifMatch == "" {
		in.IfNoneMatch = awsv2.String("*")
		return
	}
	in.IfMatch = awsv2.String(*ifMatch)
}

// conditionalOpts returns per-call options for conditional writes.
// On Tigris Global/Dual-region buckets, conditional writes are
// otherwise evaluated against the REGIONAL replica and resolved
// last-writer-wins across regions — concurrent CAS from different
// regions can all appear to succeed. X-Tigris-Consistent routes the
// precondition evaluation to the global leader, restoring linearizable CAS.
// Non-Tigris backends ignore the header.
func conditionalOpts(ifMatch *string) []func(*s3.Options) {
	if ifMatch == nil {
		return nil
	}
	return []func(*s3.Options){tigrisConsistent}
}

// tigrisConsistent sets X-Tigris-Consistent so the request is evaluated
// against the global leader rather than the local regional replica.
func tigrisConsistent(o *s3.Options) {
	o.APIOptions = append(o.APIOptions,
		smithyhttp.SetHeaderValue("X-Tigris-Consistent", "true"))
}

func (a *S3) Put(ctx context.Context, key string, body io.Reader, length int64, ifMatch *string) (string, error) {
	in := &s3.PutObjectInput{
		Bucket:        awsv2.String(a.bucket),
		Key:           awsv2.String(a.keyOf(key)),
		Body:          body,
		ContentLength: awsv2.Int64(length),
	}
	applyIfMatch(in, ifMatch)
	if cc := cacheControlFromContext(ctx); cc != "" {
		in.CacheControl = awsv2.String(cc)
	}
	out, err := a.client.PutObject(ctx, in, conditionalOpts(ifMatch)...)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrPreconditionFailed
		}
		return "", fmt.Errorf("objectstore/s3: Put %q: %w", key, err)
	}
	return strippedETag(out.ETag), nil
}

func (a *S3) PutStream(ctx context.Context, key string, body io.Reader, ifMatch *string) (string, error) {
	in := &s3.PutObjectInput{
		Bucket: awsv2.String(a.bucket),
		Key:    awsv2.String(a.keyOf(key)),
		Body:   body,
	}
	applyIfMatch(in, ifMatch)
	if cc := cacheControlFromContext(ctx); cc != "" {
		in.CacheControl = awsv2.String(cc)
	}
	var upOpts []func(*manager.Uploader)
	if opts := conditionalOpts(ifMatch); opts != nil {
		upOpts = append(upOpts, manager.WithUploaderRequestOptions(opts...))
	}
	out, err := a.uploader.Upload(ctx, in, upOpts...)
	if err != nil {
		if isPreconditionFailed(err) {
			return "", ErrPreconditionFailed
		}
		return "", fmt.Errorf("objectstore/s3: PutStream %q: %w", key, err)
	}
	return strippedETag(out.ETag), nil
}

func (a *S3) Get(ctx context.Context, key string) (io.ReadCloser, string, error) {
	var opts []func(*s3.Options)
	if consistentReadFromContext(ctx) {
		opts = append(opts, tigrisConsistent)
	}
	out, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: awsv2.String(a.bucket),
		Key:    awsv2.String(a.keyOf(key)),
	}, opts...)
	if err != nil {
		if isNotFound(err) {
			return nil, "", ErrNotFound
		}
		return nil, "", fmt.Errorf("objectstore/s3: Get %q: %w", key, err)
	}
	return out.Body, strippedETag(out.ETag), nil
}

func (a *S3) GetRange(ctx context.Context, key string, off, length int64) (io.ReadCloser, error) {
	rangeHeader := formatRange(off, length)
	out, err := a.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: awsv2.String(a.bucket),
		Key:    awsv2.String(a.keyOf(key)),
		Range:  awsv2.String(rangeHeader),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("objectstore/s3: GetRange %q %s: %w", key, rangeHeader, err)
	}
	return out.Body, nil
}

func (a *S3) List(ctx context.Context, prefix, startAfter string) ([]ObjectInfo, error) {
	in := &s3.ListObjectsV2Input{
		Bucket: awsv2.String(a.bucket),
		Prefix: awsv2.String(a.keyOf(prefix)),
	}
	if startAfter != "" {
		in.StartAfter = awsv2.String(a.keyOf(startAfter))
	}
	out, err := a.client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("objectstore/s3: List %q: %w", prefix, err)
	}
	result := make([]ObjectInfo, 0, len(out.Contents))
	for _, c := range out.Contents {
		if c.Key == nil {
			continue
		}
		k := *c.Key
		if !strings.HasPrefix(k, a.prefix) {
			continue
		}
		info := ObjectInfo{Key: k[len(a.prefix):], ETag: strippedETag(c.ETag)}
		if c.Size != nil {
			info.Size = *c.Size
		}
		if c.LastModified != nil {
			info.LastModified = *c.LastModified
		}
		result = append(result, info)
	}
	return result, nil
}

func (a *S3) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	out, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: awsv2.String(a.bucket),
		Key:    awsv2.String(a.keyOf(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return ObjectInfo{}, ErrNotFound
		}
		return ObjectInfo{}, fmt.Errorf("objectstore/s3: Stat %q: %w", key, err)
	}
	info := ObjectInfo{Key: key, ETag: strippedETag(out.ETag)}
	if out.ContentLength != nil {
		info.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		info.LastModified = *out.LastModified
	}
	return info, nil
}

func (a *S3) Delete(ctx context.Context, key string) error {
	// S3 DeleteObject is a no-op on missing keys. To honor the Bucket
	// contract (ErrNotFound on missing), HEAD first.
	_, err := a.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: awsv2.String(a.bucket),
		Key:    awsv2.String(a.keyOf(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return fmt.Errorf("objectstore/s3: Delete head %q: %w", key, err)
	}
	if _, err := a.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: awsv2.String(a.bucket),
		Key:    awsv2.String(a.keyOf(key)),
	}); err != nil {
		return fmt.Errorf("objectstore/s3: Delete %q: %w", key, err)
	}
	return nil
}

func strippedETag(s *string) string {
	if s == nil {
		return ""
	}
	return strings.Trim(*s, `"`)
}

// formatRange builds an HTTP Range header. off<0 → "from the end"
// (suffix range). length<=0 with off>=0 → "to end of object."
func formatRange(off, length int64) string {
	if off < 0 {
		return fmt.Sprintf("bytes=%d", off)
	}
	if length <= 0 {
		return fmt.Sprintf("bytes=%d-", off)
	}
	return fmt.Sprintf("bytes=%d-%d", off, off+length-1)
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	if errors.As(err, &nf) {
		return true
	}
	var resp *smithyhttp.ResponseError
	if errors.As(err, &resp) && resp.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	return false
}

func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	var resp *smithyhttp.ResponseError
	if errors.As(err, &resp) && resp.HTTPStatusCode() == http.StatusPreconditionFailed {
		return true
	}
	return false
}
