package util

import (
	"testing"

	. "github.com/onsi/gomega"
	"github.com/pingcap/tidb-operator/pkg/apis/pingcap/v1alpha1"
)

func TestNewS3StorageOption(t *testing.T) {
	g := NewGomegaWithT(t)

	qs := &s3Query{
		bucket:       "bname",
		prefix:       "root/",
		region:       "region",
		provider:     "provider",
		endpoint:     "endpoint",
		sse:          "sse",
		acl:          "acl",
		storageClass: "storageClass",
	}

	ops := []string{
		"--storage=s3://bname/root/",
		"--s3.region=region",
		"--s3.provider=provider",
		"--s3.endpoint=endpoint",
		"--s3.sse=sse",
		"--s3.acl=acl",
		"--s3.storage-class=storageClass",
	}

	get := newS3StorageOption(qs)
	g.Expect(get).Should(Equal(ops))
}

func TestNewS3Storage(t *testing.T) {
	g := NewGomegaWithT(t)
	qs := &s3Query{}

	_, err := newS3Storage(qs)
	g.Expect(err).ShouldNot(BeNil())

	qs.bucket = "bname"
	_, err = newS3Storage(qs)
	g.Expect(err).Should(BeNil())
}

func TestNewGCSStorageOption(t *testing.T) {
	g := NewGomegaWithT(t)

	tests := []struct {
		qs     *gcsQuery
		expect []string
	}{
		{
			qs: &gcsQuery{
				bucket:       "bk",
				prefix:       "/",
				storageClass: "class",
			},
			expect: []string{
				"--storage=gcs://bk/",
				"--gcs.storage-class=class",
			},
		},
		{
			qs: &gcsQuery{
				bucket:    "bk",
				prefix:    "aa/",
				objectAcl: "acl",
			},
			expect: []string{
				"--storage=gcs://bk/aa/",
				"--gcs.predefined-acl=acl",
			},
		},
	}

	for _, test := range tests {
		get := newGcsStorageOption(test.qs)
		g.Expect(get).Should(Equal(test.expect))
	}
}

func TestS3Config(t *testing.T) {
	g := NewGomegaWithT(t)

	tests := []struct {
		name       string
		s3         *v1alpha1.S3StorageProvider
		fakeRegion bool
		expect     *s3Query
	}{
		{
			name: "test alibaba provider",
			s3: &v1alpha1.S3StorageProvider{
				Provider: "alibaba",
				Prefix:   "/",
			},
			fakeRegion: false,
			expect: &s3Query{
				provider:       "alibaba",
				forcePathStyle: false,
				prefix:         "/",
			},
		},
		{
			name: "test netease provider",
			s3: &v1alpha1.S3StorageProvider{
				Provider: "netease",
				Prefix:   "/",
			},
			fakeRegion: false,
			expect: &s3Query{
				provider:       "netease",
				forcePathStyle: false,
				prefix:         "/",
			},
		},
		{
			name: "test fakeRegion",
			s3: &v1alpha1.S3StorageProvider{
				Provider: "pingcap",
				Prefix:   "/",
			},
			fakeRegion: true,
			expect: &s3Query{
				provider:       "pingcap",
				forcePathStyle: true,
				prefix:         "/",
				region:         "us-east-1",
			},
		},
	}

	for _, test := range tests {
		t.Log(test.name)
		get := checkS3Config(test.s3, test.fakeRegion)
		g.Expect(get).Should(Equal(test.expect))
	}
}

func TestGCSConfig(t *testing.T) {
	g := NewGomegaWithT(t)

	tests := []struct {
		gcs    *v1alpha1.GcsStorageProvider
		expect *gcsQuery
	}{
		{
			gcs: &v1alpha1.GcsStorageProvider{
				Prefix: "/",
			},
			expect: &gcsQuery{
				prefix: "/",
			},
		},
		{
			gcs: &v1alpha1.GcsStorageProvider{
				Prefix: "///a///",
			},
			expect: &gcsQuery{
				prefix: "a/",
			},
		},
		{
			gcs: &v1alpha1.GcsStorageProvider{
				Prefix: "///a",
			},
			expect: &gcsQuery{
				prefix: "a/",
			},
		},
	}

	for _, test := range tests {
		get := checkGcsConfig(test.gcs, false)
		g.Expect(get).Should(Equal(test.expect))
	}
}
