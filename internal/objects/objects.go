// Package objects stores and reads files in the S3-compatible object store.
package objects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// ErrNotFound means there is no object under that key.
var ErrNotFound = errors.New("object not found")

// Store is the object store.
type Store struct {
	client *minio.Client
}

// Open prepares a client for the store at endpoint ("http://localhost:9000").
func Open(endpoint, accessKey, secretKey, region string) (*Store, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return nil, fmt.Errorf("S3_ENDPOINT %q is not an address like http://localhost:9000", endpoint)
	}
	client, err := minio.New(parsed.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       parsed.Scheme == "https",
		Region:       region,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("object store: %w", err)
	}
	return &Store{client: client}, nil
}

// CheckBuckets reports the first of the buckets that does not exist or cannot be reached.
func (s *Store) CheckBuckets(ctx context.Context, buckets ...string) error {
	for _, bucket := range buckets {
		exists, err := s.client.BucketExists(ctx, bucket)
		if err != nil {
			return fmt.Errorf("object store: %w", err)
		}
		if !exists {
			return fmt.Errorf("bucket %s does not exist; create the buckets first (likho-infra: scripts/up.sh)", bucket)
		}
	}
	return nil
}

// PutFile stores the file at path under bucket/key.
func (s *Store) PutFile(ctx context.Context, bucket, key, path, contentType string) error {
	_, err := s.client.FPutObject(ctx, bucket, key, path, minio.PutObjectOptions{ContentType: contentType})
	return err
}

// PutBytes stores data under bucket/key.
func (s *Store) PutBytes(ctx context.Context, bucket, key string, data io.Reader, size int64, contentType string) error {
	_, err := s.client.PutObject(ctx, bucket, key, data, size, minio.PutObjectOptions{ContentType: contentType})
	return err
}

// GetFile writes the object bucket/key to path.
func (s *Store) GetFile(ctx context.Context, bucket, key, path string) error {
	return notFound(s.client.FGetObject(ctx, bucket, key, path, minio.GetObjectOptions{}))
}

// Object is an open object: it can be read from any position, which is what serving
// a browser's range requests needs.
type Object struct {
	io.ReadSeekCloser
	Size        int64
	ContentType string
	ETag        string
}

// Open opens the object bucket/key for reading.
func (s *Store) Open(ctx context.Context, bucket, key string) (*Object, error) {
	object, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, notFound(err)
	}
	info, err := object.Stat()
	if err != nil {
		_ = object.Close()
		return nil, notFound(err)
	}
	return &Object{ReadSeekCloser: object, Size: info.Size, ContentType: info.ContentType, ETag: info.ETag}, nil
}

// Remove deletes the object. Removing an object that does not exist is not an error.
func (s *Store) Remove(ctx context.Context, bucket, key string) error {
	if key == "" {
		return nil
	}
	return s.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
}

func notFound(err error) error {
	if err == nil {
		return nil
	}
	if code := minio.ToErrorResponse(err).Code; code == "NoSuchKey" || code == "NoSuchBucket" {
		return ErrNotFound
	}
	return err
}
