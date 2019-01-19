package gofakes3_test

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/johannesboyne/gofakes3"
)

func TestCreateBucket(t *testing.T) {
	//@TODO(jb): implement them for sanity reasons

	ts := newTestServer(t)
	defer ts.Close()

	svc := ts.s3Client()

	ts.OKAll(svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("testbucket"),
	}))
	ts.OKAll(svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String("testbucket"),
	}))
	ts.OKAll(svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String("testbucket"),
		Key:    aws.String("ObjectKey"),
		Body:   bytes.NewReader([]byte(`{"test": "foo"}`)),
		Metadata: map[string]*string{
			"Key": aws.String("MetadataValue"),
		},
	}))
	ts.OKAll(svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("testbucket"),
		Key:    aws.String("ObjectKey"),
	}))
}

func TestListBuckets(t *testing.T) {
	ts := newTestServer(t, withoutInitialBuckets())
	defer ts.Close()
	svc := ts.s3Client()

	assertBuckets := func(expected ...string) {
		t.Helper()
		rs, err := svc.ListBuckets(&s3.ListBucketsInput{})
		ts.OK(err)

		var found []string
		for _, bucket := range rs.Buckets {
			found = append(found, *bucket.Name)
		}

		sort.Strings(expected)
		sort.Strings(found)
		if !reflect.DeepEqual(found, expected) {
			t.Fatalf("buckets:\nexp: %v\ngot: %v", expected, found)
		}
	}

	assertBucketTime := func(bucket string, created time.Time) {
		t.Helper()
		rs, err := svc.ListBuckets(&s3.ListBucketsInput{})
		ts.OK(err)

		for _, v := range rs.Buckets {
			if *v.Name == bucket {
				if *v.CreationDate != created {
					t.Fatal("time mismatch for bucket", bucket, "expected:", created, "found:", *v.CreationDate)
				}
				return
			}
		}
		t.Fatal("bucket", bucket, "not found")
	}

	assertBuckets()

	ts.backendCreateBucket("test")
	assertBuckets("test")
	assertBucketTime("test", defaultDate)

	ts.backendCreateBucket("test2")
	assertBuckets("test", "test2")
	assertBucketTime("test2", defaultDate)

	ts.Advance(1 * time.Minute)

	ts.backendCreateBucket("test3")
	assertBuckets("test", "test2", "test3")

	assertBucketTime("test", defaultDate)
	assertBucketTime("test2", defaultDate)
	assertBucketTime("test3", defaultDate.Add(1*time.Minute))
}

func TestCreateObject(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	ts.OKAll(svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(defaultBucket),
		Key:    aws.String("object"),
		Body:   bytes.NewReader([]byte("hello")),
	}))

	obj := ts.backendGetString(defaultBucket, "object", nil)
	if obj != "hello" {
		t.Fatal("object creation failed")
	}
}

func TestCreateObjectMD5(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	svc := ts.s3Client()

	{ // md5 is valid base64 but does not match content:
		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket:     aws.String(defaultBucket),
			Key:        aws.String("invalid"),
			Body:       bytes.NewReader([]byte("hello")),
			ContentMD5: aws.String("bnVwCg=="),
		})
		if !s3HasErrorCode(err, gofakes3.ErrBadDigest) {
			t.Fatal("expected BadDigest error, found", err)
		}
	}

	{ // md5 is invalid base64:
		_, err := svc.PutObject(&s3.PutObjectInput{
			Bucket:     aws.String(defaultBucket),
			Key:        aws.String("invalid"),
			Body:       bytes.NewReader([]byte("hello")),
			ContentMD5: aws.String("!*@&(*$&"),
		})
		if !s3HasErrorCode(err, gofakes3.ErrInvalidDigest) {
			t.Fatal("expected InvalidDigest error, found", err)
		}
	}

	if ts.backendObjectExists(defaultBucket, "invalid") {
		t.Fatal("unexpected object")
	}
}

func TestDeleteBucket(t *testing.T) {
	t.Run("delete-empty", func(t *testing.T) {
		ts := newTestServer(t, withoutInitialBuckets())
		defer ts.Close()
		svc := ts.s3Client()

		ts.backendCreateBucket("test")
		ts.OKAll(svc.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String("test"),
		}))
	})

	t.Run("delete-fails-if-not-empty", func(t *testing.T) {
		ts := newTestServer(t, withoutInitialBuckets())
		defer ts.Close()
		svc := ts.s3Client()

		ts.backendCreateBucket("test")
		ts.backendPutString("test", "test", nil, "test")
		_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
			Bucket: aws.String("test"),
		})
		if !hasErrorCode(err, gofakes3.ErrBucketNotEmpty) {
			t.Fatal("expected ErrBucketNotEmpty, found", err)
		}
	})
}

func TestDeleteMulti(t *testing.T) {
	deletedKeys := func(rs *s3.DeleteObjectsOutput) []string {
		deleted := make([]string, len(rs.Deleted))
		for idx, del := range rs.Deleted {
			deleted[idx] = *del.Key
		}
		sort.Strings(deleted)
		return deleted
	}

	assertDeletedKeys := func(t *testing.T, rs *s3.DeleteObjectsOutput, expected ...string) {
		t.Helper()
		found := deletedKeys(rs)
		if !reflect.DeepEqual(found, expected) {
			t.Fatal("multi deletion failed", found, "!=", expected)
		}
	}

	t.Run("one-file", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		svc := ts.s3Client()

		ts.backendPutString(defaultBucket, "foo", nil, "one")
		ts.backendPutString(defaultBucket, "bar", nil, "two")
		ts.backendPutString(defaultBucket, "baz", nil, "three")

		rs, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(defaultBucket),
			Delete: &s3.Delete{
				Objects: []*s3.ObjectIdentifier{
					{Key: aws.String("foo")},
				},
			},
		})
		ts.OK(err)
		assertDeletedKeys(t, rs, "foo")
		ts.assertLs(defaultBucket, "", nil, []string{"bar", "baz"})
	})

	t.Run("multiple-files", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		svc := ts.s3Client()

		ts.backendPutString(defaultBucket, "foo", nil, "one")
		ts.backendPutString(defaultBucket, "bar", nil, "two")
		ts.backendPutString(defaultBucket, "baz", nil, "three")

		rs, err := svc.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(defaultBucket),
			Delete: &s3.Delete{
				Objects: []*s3.ObjectIdentifier{
					{Key: aws.String("bar")},
					{Key: aws.String("foo")},
				},
			},
		})
		ts.OK(err)
		assertDeletedKeys(t, rs, "bar", "foo")
		ts.assertLs(defaultBucket, "", nil, []string{"baz"})
	})
}

func TestGetObjectRange(t *testing.T) {
	assertRange := func(ts *testServer, key string, hdr string, expected []byte) {
		svc := ts.s3Client()
		obj, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(defaultBucket),
			Key:    aws.String(key),
			Range:  aws.String(hdr),
		})
		ts.OK(err)
		defer obj.Body.Close()

		out, err := ioutil.ReadAll(obj.Body)
		ts.OK(err)
		if !bytes.Equal(expected, out) {
			ts.Fatal("range failed", hdr, err)
		}
	}

	in := randomFileBody(1024)

	for idx, tc := range []struct {
		hdr      string
		expected []byte
	}{
		{"bytes=0-", in},
		{"bytes=1-", in[1:]},
		{"bytes=0-0", in[:1]},
		{"bytes=0-1", in[:2]},
		{"bytes=1023-1023", in[1023:1024]},

		// if the requested end is beyond the real end, it should still work
		{"bytes=1023-1024", in[1023:1024]},

		// if the requested start is beyond the real end, it should still work
		{"bytes=1024-1024", []byte{}},

		// suffix-byte-range-spec:
		{"bytes=-0", []byte{}},
		{"bytes=-1", in[1023:1024]},
		{"bytes=-1024", in},
		{"bytes=-1025", in},
	} {
		t.Run(fmt.Sprintf("%d/%s", idx, tc.hdr), func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			ts.backendPutBytes(defaultBucket, "foo", nil, in)
			assertRange(ts, "foo", tc.hdr, tc.expected)
		})
	}
}

func TestGetObjectRangeInvalid(t *testing.T) {
	assertRangeInvalid := func(ts *testServer, key string, hdr string) {
		svc := ts.s3Client()
		_, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(defaultBucket),
			Key:    aws.String(key),
			Range:  aws.String(hdr),
		})
		if !hasErrorCode(err, gofakes3.ErrInvalidRange) {
			ts.Fatal("expected ErrInvalidRange, found", err)
		}
	}

	in := randomFileBody(1024)

	for idx, tc := range []struct {
		hdr string
	}{
		{"boats=0-0"},
		{"bytes="},
		{"bytes=0"},
		{"bytes=0-1,1-2"}, // multiple ranges invalid
		{"bytes=-quack"},
		{"bytes=quack-1"},
		{"bytes=1-quack"},
		{"bytes=9223372036854775808-"}, // int64 overflow
	} {
		t.Run(fmt.Sprintf("%d/%s", idx, tc.hdr), func(t *testing.T) {
			ts := newTestServer(t)
			defer ts.Close()

			ts.backendPutBytes(defaultBucket, "foo", nil, in)
			assertRangeInvalid(ts, "foo", tc.hdr)
		})
	}
}

func TestCreateObjectBrowserUpload(t *testing.T) {
	addFile := func(tt gofakes3.TT, w *multipart.Writer, object string, b []byte) {
		tt.Helper()
		tt.OK(w.WriteField("key", object))

		mw, err := w.CreateFormFile("file", "upload")
		tt.OK(err)
		n, err := mw.Write(b)
		if n != len(b) {
			tt.Fatal("len mismatch", n, "!=", len(b))
		}
		tt.OK(err)
	}

	upload := func(ts *testServer, bucket string, w *multipart.Writer, body io.Reader) (*http.Response, error) {
		w.Close()
		req, err := http.NewRequest("POST", ts.url("/"+bucket), body)
		ts.OK(err)
		req.Header.Set("Content-Type", w.FormDataContentType())
		return httpClient().Do(req)
	}

	assertUpload := func(ts *testServer, bucket string, w *multipart.Writer, body io.Reader) {
		res, err := upload(ts, bucket, w, body)
		ts.OK(err)
		if res.StatusCode != http.StatusOK {
			ts.Fatal("bad status", res.StatusCode)
		}
	}

	assertUploadFails := func(ts *testServer, bucket string, w *multipart.Writer, body io.Reader, expectedCode gofakes3.ErrorCode) {
		res, err := upload(ts, bucket, w, body)
		ts.OK(err)
		if res.StatusCode != expectedCode.Status() {
			ts.Fatal("bad status", res.StatusCode, "!=", expectedCode.Status())
		}
		defer res.Body.Close()
		var errResp gofakes3.ErrorResponse
		dec := xml.NewDecoder(res.Body)
		ts.OK(dec.Decode(&errResp))

		if errResp.Code != expectedCode {
			ts.Fatal("bad code", errResp.Code, "!=", expectedCode)
		}
	}

	t.Run("single-upload", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		addFile(ts.TT, w, "yep", []byte("stuff"))
		assertUpload(ts, defaultBucket, w, &b)
		ts.assertObject(defaultBucket, "yep", nil, "stuff")
	})

	t.Run("multiple-files-fails", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		addFile(ts.TT, w, "yep", []byte("stuff"))
		addFile(ts.TT, w, "nup", []byte("bork"))
		assertUploadFails(ts, defaultBucket, w, &b, gofakes3.ErrIncorrectNumberOfFilesInPostRequest)
	})

	t.Run("key-too-large", func(t *testing.T) {
		ts := newTestServer(t)
		defer ts.Close()
		var b bytes.Buffer
		w := multipart.NewWriter(&b)
		addFile(ts.TT, w, strings.Repeat("a", gofakes3.KeySizeLimit+1), []byte("yep"))
		assertUploadFails(ts, defaultBucket, w, &b, gofakes3.ErrKeyTooLong)
	})
}

func s3HasErrorCode(err error, code gofakes3.ErrorCode) bool {
	if err, ok := err.(awserr.Error); ok {
		return code == gofakes3.ErrorCode(err.Code())
	}
	return false
}
