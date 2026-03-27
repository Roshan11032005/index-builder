// Package s3store wraps the AWS SDK S3 client with domain-specific helpers.
package s3store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	appcfg "index-builder/config"
)

// Client wraps an S3 client for a specific bucket.
type Client struct {
	bucket string
	s3     *s3.Client
}

// New creates an S3 Client for the given BucketConfig.
func New(cfg appcfg.BucketConfig) (*Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("s3store: load config: %w", err)
	}

	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		//nolint:staticcheck // EndpointResolver is deprecated but still works for MinIO / custom endpoints
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(_ string, _ s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.Endpoint,
					HostnameImmutable: true,
					SigningRegion:     cfg.Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})

	return &Client{bucket: cfg.Name, s3: cli}, nil
}

// BucketName returns the bucket this client is bound to.
func (c *Client) BucketName() string { return c.bucket }

// ─── Object operations ────────────────────────────────────────────────────────

// GetBytes downloads an object and returns its raw bytes.
func (c *Client) GetBytes(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3store: get %q: %w", key, err)
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// GetReader downloads an object and returns a streaming reader.
// The caller is responsible for closing the body.
func (c *Client) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3store: get stream %q: %w", key, err)
	}
	return resp.Body, nil
}

// PutBytes uploads raw bytes to key with the given content type and metadata.
func (c *Client) PutBytes(ctx context.Context, key, contentType string, data []byte, meta map[string]string) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
		Metadata:    meta,
	})
	if err != nil {
		return fmt.Errorf("s3store: put %q: %w", key, err)
	}
	return nil
}

// PutJSON marshals v and uploads it as application/json to key.
func (c *Client) PutJSON(ctx context.Context, key string, v any, meta map[string]string) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("s3store: marshal for %q: %w", key, err)
	}
	return c.PutBytes(ctx, key, "application/json", data, meta)
}

// GetJSON downloads key and JSON-decodes it into v.
func (c *Client) GetJSON(ctx context.Context, key string, v any) error {
	data, err := c.GetBytes(ctx, key)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("s3store: decode %q: %w", key, err)
	}
	return nil
}

// DeleteMany removes up to 1 000 objects per API call.
func (c *Client) DeleteMany(ctx context.Context, keys []string) error {
	const batchSize = 1000
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		ids := make([]types.ObjectIdentifier, end-i)
		for j, k := range keys[i:end] {
			ids[j] = types.ObjectIdentifier{Key: aws.String(k)}
		}
		batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := c.s3.DeleteObjects(batchCtx, &s3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &types.Delete{Objects: ids},
		})
		cancel()
		if err != nil {
			return fmt.Errorf("s3store: delete batch: %w", err)
		}
	}
	return nil
}

// ListWithPrefix returns all object keys under the given prefix.
func (c *Client) ListWithPrefix(ctx context.Context, prefix string) ([]types.Object, error) {
	return c.ListWithPrefixAfter(ctx, prefix, "")
}

// ListWithPrefixAfter returns objects under prefix whose keys are
// lexicographically after startAfter. This avoids scanning already-processed
// objects and is critical for buckets with millions of keys.
func (c *Client) ListWithPrefixAfter(ctx context.Context, prefix, startAfter string) ([]types.Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}

	var out []types.Object
	pager := s3.NewListObjectsV2Paginator(c.s3, input)
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3store: list %q: %w", prefix, err)
		}
		out = append(out, page.Contents...)
	}
	return out, nil
}

// Object is re-exported from the AWS SDK types package for use by callers that
// only import this package.
type Object = types.Object