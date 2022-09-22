package common

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"io"
	"log"
	"os"
	"time"
)

func CreateGCSClient(ctx context.Context) *storage.Client {
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("→ GCS →→ storage.NewClient: %v", err)
	}
	return client
}

func GetBucket(client *storage.Client, bucket string) *storage.BucketHandle {
	return client.Bucket(bucket)
}

func CopyGCSObject(
	ctx context.Context,
	srcBucket string,
	srcDirectory string,
	srcObjectName string,
	dstBucket string,
	dstDirectory string,
	dstObjectName string,
) {
	client := CreateGCSClient(ctx)
	defer client.Close()

	srcObject := srcDirectory + "/" + srcObjectName
	src := client.Bucket(srcBucket).Object(srcObject)

	if dstBucket == "" {
		dstBucket = srcBucket
	}

	if dstDirectory == "" {
		dstDirectory = srcDirectory
	}

	dstObject := dstDirectory + "/" + dstObjectName
	dst := client.Bucket(dstBucket).Object(dstObject)

	log.Printf("→ GCS →→ Copy file from [%s/%s] to [%s/%s]", srcBucket, srcObject, dstBucket, dstObject)
	if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
		log.Fatalf("→ GCS →→ Object(%q).CopierFrom(%q).Run: %v", dstObject, srcObject, err)
	}
}

func UploadLocalFileToABucket(
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

	client := CreateGCSClient(ctx)
	defer client.Close()

	dstBkt := GetBucket(client, bucket)
	o := dstBkt.Object(bucketDirectory + "/" + objectName)

	wc := o.NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		log.Fatalf("→ GCS →→ io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		log.Fatalf("→ GCS →→ Writer.Close: %v", err)
	}
}

func ListGCSBucketObjects(
	ctx context.Context,
	bucket string,
	bucketDirectory string,
) []string {
	ctx = context.Background()

	client := CreateGCSClient(ctx)
	defer client.Close()

	bkt := client.Bucket(bucket)
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

func MergeObjects(ctx context.Context, bucket string, objectNames []string, mergedObjectName string) {
	client := CreateGCSClient(ctx)
	defer client.Close()
	bkt := GetBucket(client, bucket)
	for _, name := range objectNames {
		if name != mergedObjectName {
			src1 := bkt.Object(name)
			src2 := bkt.Object(mergedObjectName)
			dst := bkt.Object(mergedObjectName)

			_, err := dst.ComposerFrom(src1, src2).Run(ctx)
			if err != nil {
				log.Fatalf("→ GCS →→ ComposerFrom: %v", err)
			}
			log.Printf("→ GCS →→ New composite object %v was created by combining %v and %v\n", mergedObjectName, name, mergedObjectName)
			DeleteObject(ctx, bucket, name)
		}
	}
}

func DeleteObject(ctx context.Context, bucket string, object string) {
	log.Printf("→ GCS →→ Deleting object [%s/%s]", bucket, object)
	client := CreateGCSClient(ctx)
	defer client.Close()
	bkt := GetBucket(client, bucket)
	obj := bkt.Object(object)
	if err := obj.Delete(ctx); err != nil {
		log.Fatalf(" → GCS →→ Cannot delete object with name %s", object)
	}
}
