package main

import (
	"context"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/kelseyhightower/envconfig"
	"net/http"
)

var (
	cfg Config
)

type Config struct {
	S3FileBucket string `envconfig:"S3_FILE_BUCKET" required:"true"`
}

type ImportEvent struct {
	URL      string            `json:"url"`
	FileName string            `json:"file_name"`
	Headers  map[string]string `json:"headers"`
}

func download(url string, headers map[string]string) (response *http.Response, err error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for header, value := range headers {
		req.Header.Add(header, value)
	}

	client := http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: 500,
		},
	}
	return client.Do(req)
}

func HandleRequest(ctx context.Context, event ImportEvent) (string, error) {
	sess := session.Must(session.NewSession())

	response, err := download(event.URL, event.Headers)
	if err != nil {
		return "", err
	}

	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024 // The minimum/default allowed part size is 5MB
		u.Concurrency = 5            // default is 5
	})

	result, err := uploader.UploadWithContext(
		ctx,
		&s3manager.UploadInput{
			Bucket: aws.String(cfg.S3FileBucket),
			Key:    aws.String(event.FileName),
			Body:   response.Body,
		},
	)
	if err != nil {
		return "", err
	}

	return result.Location, nil
}

func main() {
	err := envconfig.Process("", &cfg)
	if err != nil {
		panic(err)
	}

	lambda.Start(HandleRequest)
}
