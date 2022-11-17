package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kelseyhightower/envconfig"
	"io"
	"net/http"
	"os"
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

func cache(src io.Reader) (*os.File, error) {
	fp, err := os.CreateTemp("", "")
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(fp, src)
	if err != nil {
		return nil, err
	}
	_, err = fp.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return fp, nil
}

func HandleRequest(ctx context.Context, event ImportEvent) (string, error) {
	sess := session.Must(session.NewSession())
	s3Client := s3.New(sess)

	response, err := download(event.URL, event.Headers)
	if err != nil {
		return "", err
	}

	fp, err := cache(response.Body)
	if err != nil {
		return "", err
	}
	defer func(fp *os.File) {
		err := fp.Close()
		if err != nil {
			fmt.Printf("failed to close the file %s: %v\n", event.FileName, err)
		}
	}(fp)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			fmt.Printf("failed to delete file %s: %v\n", name, err)
		}
	}(fp.Name())

	out, err := s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.S3FileBucket),
		Key:    aws.String(event.FileName),
		Body:   fp,
	})
	if err != nil {
		return "", err
	}

	return out.String(), nil
}

func main() {
	err := envconfig.Process("", &cfg)
	if err != nil {
		panic(err)
	}

	lambda.Start(HandleRequest)
}
