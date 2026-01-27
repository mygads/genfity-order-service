package storage

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	PublicBaseURL   string
	StorageClass    string
}

type ObjectStore struct {
	bucket       string
	publicBase   string
	storageClass string
	client       *s3.Client
}

func NewObjectStore(ctx context.Context, cfg Config) (*ObjectStore, error) {
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("object store endpoint is required")
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}

	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		region = "auto"
	}

	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, fmt.Errorf("object store bucket is required")
	}

	publicBase := strings.TrimRight(strings.TrimSpace(cfg.PublicBaseURL), "/")
	if publicBase == "" {
		return nil, fmt.Errorf("object store public base url is required")
	}

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...any) (aws.Endpoint, error) {
		if service == s3.ServiceID {
			return aws.Endpoint{URL: endpoint, HostnameImmutable: true}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			strings.TrimSpace(cfg.AccessKeyID),
			strings.TrimSpace(cfg.SecretAccessKey),
			"",
		)),
		awsconfig.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		// Cloudflare R2 (S3-compatible) generally requires path-style.
		o.UsePathStyle = true
	})

	return &ObjectStore{
		bucket:       strings.TrimSpace(cfg.Bucket),
		publicBase:   publicBase,
		storageClass: strings.TrimSpace(cfg.StorageClass),
		client:       client,
	}, nil
}

func (s *ObjectStore) PublicURL(key string) string {
	key = strings.TrimLeft(key, "/")
	return s.publicBase + "/" + key
}

func (s *ObjectStore) PutObject(ctx context.Context, key string, body []byte, contentType string, cacheControl string) (string, error) {
	key = strings.TrimLeft(key, "/")
	ct := strings.TrimSpace(contentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	cc := strings.TrimSpace(cacheControl)
	if cc == "" {
		cc = "public, max-age=31536000, immutable"
	}

	input := &s3.PutObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(body),
		ContentType:  aws.String(ct),
		CacheControl: aws.String(cc),
	}

	if sc := parseStorageClass(s.storageClass); sc != nil {
		input.StorageClass = *sc
	}

	if _, err := s.client.PutObject(ctx, input); err != nil {
		return "", err
	}

	return s.PublicURL(key), nil
}

func (s *ObjectStore) PresignPutObject(ctx context.Context, key string, contentType string, cacheControl string, expires time.Duration) (string, error) {
	key = strings.TrimLeft(key, "/")
	ct := strings.TrimSpace(contentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	cc := strings.TrimSpace(cacheControl)
	if cc == "" {
		cc = "public, max-age=31536000, immutable"
	}
	if expires <= 0 {
		expires = 15 * time.Minute
	}

	input := &s3.PutObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ContentType:  aws.String(ct),
		CacheControl: aws.String(cc),
	}

	if sc := parseStorageClass(s.storageClass); sc != nil {
		input.StorageClass = *sc
	}

	p := s3.NewPresignClient(s.client)
	out, err := p.PresignPutObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", err
	}

	return out.URL, nil
}

func (s *ObjectStore) ListKeys(ctx context.Context, prefix string) ([]string, error) {
	prefix = strings.TrimLeft(prefix, "/")
	var out []string
	var token *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Contents {
			if item.Key == nil {
				continue
			}
			out = append(out, *item.Key)
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
		token = resp.NextContinuationToken
	}
	return out, nil
}

func (s *ObjectStore) DeleteKey(ctx context.Context, key string) error {
	key = strings.TrimLeft(key, "/")
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *ObjectStore) DeletePrefix(ctx context.Context, prefix string) error {
	keys, err := s.ListKeys(ctx, prefix)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := s.DeleteKey(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *ObjectStore) ResolveKeyFromURL(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false
	}
	if strings.HasPrefix(raw, s.publicBase+"/") {
		return strings.TrimLeft(raw[len(s.publicBase):], "/"), true
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}

	// R2 S3-style URL can be: https://<account>.r2.cloudflarestorage.com/<bucket>/<key>
	parts := strings.Split(strings.TrimLeft(parsed.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == s.bucket {
		return strings.Join(parts[1:], "/"), true
	}

	return "", false
}

func (s *ObjectStore) DeleteURL(ctx context.Context, raw string) error {
	key, ok := s.ResolveKeyFromURL(raw)
	if !ok {
		return fmt.Errorf("unmanaged url")
	}
	return s.DeleteKey(ctx, key)
}

func parseStorageClass(v string) *types.StorageClass {
	v = strings.TrimSpace(strings.ToUpper(v))
	if v == "" {
		return nil
	}
	sc := types.StorageClass(v)
	return &sc
}
