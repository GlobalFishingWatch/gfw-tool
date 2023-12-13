package common

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func GCSCreateClient(ctx context.Context) *storage.Client {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("→ GCS →→ storage.NewClient: %v", err)
	}
	return client
}

func GCSGetBucket(ctx context.Context, bucket string) *storage.BucketHandle {
	client := GCSCreateClient(ctx)
	defer client.Close()
	return client.Bucket(bucket)
}

func GCSCopyObject(
	ctx context.Context,
	srcBucket string,
	srcDirectory string,
	srcObjectName string,
	dstBucket string,
	dstDirectory string,
	dstObjectName string,
) {
	sourceBucket := GCSGetBucket(ctx, srcBucket)
	srcObject := srcDirectory + "/" + srcObjectName
	src := sourceBucket.Object(srcObject)

	if dstBucket == "" {
		dstBucket = srcBucket
	}

	if dstDirectory == "" {
		dstDirectory = srcDirectory
	}

	destinationBucket := GCSGetBucket(ctx, dstBucket)
	dstObject := dstDirectory + "/" + dstObjectName
	dst := destinationBucket.Object(dstObject)

	log.Printf("→ GCS →→ Copy file from [%s/%s] to [%s/%s]", srcBucket, srcObject, dstBucket, dstObject)
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		log.Fatalf("→ GCS →→ Object(%q).CopierFrom(%q).Run: %v", dstObject, srcObject, err)
	}
}

func GCSUploadLocalFileToABucket(
	ctx context.Context,
	bucket string,
	localDirectory string,
	localFilename string,
	bucketDirectory string,
	objectName string,
) {
	log.Printf("→ GCS →→ Uploading file to [%s/%s]", bucketDirectory, objectName)
	f, err := os.Open(localDirectory + "/" + localFilename)
	if err != nil {
		log.Fatalf("→ GCS →→ os.Open: %v", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	client := GCSCreateClient(ctx)
	defer client.Close()

	dstBkt := GCSGetBucket(ctx, bucket)
	o := dstBkt.Object(bucketDirectory + "/" + objectName)

	wc := o.NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		log.Fatalf("→ GCS →→ io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		log.Fatalf("→ GCS →→ Writer.Close: %v", err)
	}
}

func GCSListBucketObjects(
	ctx context.Context,
	bucket string,
	bucketDirectory string,
) []string {

	bkt := GCSGetBucket(ctx, bucket)
	log.Printf("→ GCS →→ Listing objects")

	prefix := fmt.Sprintf(`%s/`, bucketDirectory)
	log.Printf("→ GCS →→ Prefix [%s]", prefix)
	query := &storage.Query{
		Prefix: prefix,
	}

	it := bkt.Objects(ctx, query)
	var names []string

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal("→ GCS →→ Error listing objects ", err)
		}
		names = append(names, attrs.Name)
	}

	return names
}

func GCSMergeObjects(ctx context.Context, bucket string, objectNames []string, mergedObjectName string) {
	bkt := GCSGetBucket(ctx, bucket)

	GCSCreateObjectIfNotExists(ctx, bucket, mergedObjectName, "")

	for _, name := range objectNames {
		if name != mergedObjectName {
			src1 := bkt.Object(name)
			src2 := bkt.Object(mergedObjectName)
			dst := bkt.Object(mergedObjectName)
			_, err := dst.ComposerFrom(src2, src1).Run(ctx)
			if err != nil {
				log.Fatalf("→ GCS →→ ComposerFrom: %v", err)
			}
			log.Printf("→ GCS →→ New composite object %v was created by combining %v and %v\n", mergedObjectName, name, mergedObjectName)
			GCSDeleteObject(ctx, bucket, name)

			/*
			 The ComposerFrom command is async, and we need to await a couple of seconds to about rateLimit errors because Google Cloud Storage
			 doesn't allow to make more than one request and once.
			*/
			time.Sleep(2 * time.Second)
		}
	}
}

func GCSCreateObjectIfNotExists(ctx context.Context, bucket string, object string, content string) {
	bkt := GCSGetBucket(ctx, bucket)

	ow := bkt.Object(object).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	if _, err := ow.Write([]byte(content)); err != nil {
		log.Fatalf("→ GCS →→ Error writting the object [%s]", object)
	}
	if err := ow.Close(); err != nil {
		switch e := err.(type) {
		case *googleapi.Error:
			if e.Code == http.StatusPreconditionFailed {
				log.Printf("→ GCS →→ The object with name [%s] already exists!", object)
			}
			// And others.
		default:
			log.Fatalf("→ GCS →→ Error: %s", err)
		}
	}
}

func GCSDeleteObject(ctx context.Context, bucket string, object string) {
	log.Printf("→ GCS →→ Deleting object [%s/%s]", bucket, object)
	bkt := GCSGetBucket(ctx, bucket)
	obj := bkt.Object(object)
	if err := obj.Delete(ctx); err != nil {
		log.Fatalf(" → GCS →→ Cannot delete object with name %s", object)
	}
}
