package export

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Config struct {
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
}

type Bucket struct {
	name    string
	client  *s3.Client
	presign *s3.PresignClient
}

type staticCredentials struct {
	key    string
	secret string
}

func (s staticCredentials) Retrieve(context.Context) (aws.Credentials, error) {
	return aws.Credentials{
		AccessKeyID:     s.key,
		SecretAccessKey: s.secret,
		Source:          "static",
	}, nil
}

func OpenBucket(ctx context.Context, cfg S3Config) (*Bucket, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(staticCredentials{key: cfg.AccessKey, secret: cfg.SecretKey}),
	)
	if err != nil {
		return nil, err
	}
	endpoint := cfg.Endpoint
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	return &Bucket{name: cfg.Bucket, client: client, presign: s3.NewPresignClient(client)}, nil
}

func (b *Bucket) Ensure(ctx context.Context) error {
	_, err := b.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &b.name})
	if err == nil {
		return nil
	}
	var api interface{ ErrorCode() string }
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
			return nil
		}
	}
	return err
}

func (b *Bucket) Put(ctx context.Context, key string, body io.Reader) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &b.name,
		Key:    &key,
		Body:   body,
	})
	return err
}

func (b *Bucket) PresignGet(ctx context.Context, key string) (string, error) {
	out, err := b.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &b.name,
		Key:    &key,
	}, s3.WithPresignExpires(presignTTL*time.Second))
	if err != nil {
		return "", err
	}
	return out.URL, nil
}

func (b *Bucket) List(ctx context.Context, prefix string) ([]string, error) {
	out, err := b.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &b.name,
		Prefix: &prefix,
	})
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		if obj.Key != nil {
			keys = append(keys, *obj.Key)
		}
	}
	return keys, nil
}
