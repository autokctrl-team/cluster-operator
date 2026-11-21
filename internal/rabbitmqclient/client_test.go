/*
RabbitMQ Cluster Operator

Copyright 2020 VMware, Inc. All Rights Reserved.

This product is licensed to you under the Mozilla Public license, Version 2.0 (the "License").  You may not use this product except in compliance with the Mozilla Public License.

This product may include a number of subcomponents with separate copyright notices and license terms. Your use of these subcomponents is subject to the terms and conditions of the subcomponent's license, as noted in the LICENSE file.
*/

package rabbitmqclient

import (
	"context"
	"io"
	"net/http"
	"strings"

	rabbithole "github.com/michaelklishin/rabbit-hole/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rabbitmqv1beta1 "github.com/rabbitmq/cluster-operator/v2/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

var _ = Describe("RabbitMQ Client", func() {
	var (
		ctx       context.Context
		k8sClient client.Client
		scheme    *runtime.Scheme
		rmq       *rabbitmqv1beta1.RabbitmqCluster
		secret    *corev1.Secret
		namespace string
	)

	BeforeEach(func() {
		ctx = context.Background()
		namespace = "test-namespace"

		scheme = runtime.NewScheme()
		Expect(rabbitmqv1beta1.AddToScheme(scheme)).To(Succeed())
		Expect(corev1.AddToScheme(scheme)).To(Succeed())

		rmq = &rabbitmqv1beta1.RabbitmqCluster{
			Name:      "test-cluster",
			Namespace: namespace,
			Spec:      rabbitmqv1beta1.RabbitmqClusterSpec{},
		}

		secret = &corev1.Secret{
			Name:      "test-cluster-default-user",
			Namespace: namespace,
			Data: map[string][]byte{
				"username": []byte("test-user"),
				"password": []byte("test-password"),
			},
		}
	})

	Describe("GetClientInfoForPod", func() {
		Context("when the secret exists", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, secret).
					Build()
			})

			It("returns client info with correct credentials", func() {
				podName := "test-cluster-server-0"
				info, err := getClientInfoForPod(ctx, k8sClient, rmq, podName)
				Expect(err).NotTo(HaveOccurred())
				Expect(info).NotTo(BeNil())
				Expect(info.Username).To(Equal("test-user"))
				Expect(info.Password).To(Equal("test-password"))
			})

			It("returns the correct base URL for non-TLS with pod DNS name", func() {
				podName := "test-cluster-server-0"
				info, err := getClientInfoForPod(ctx, k8sClient, rmq, podName)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.BaseURL).To(Equal("http://test-cluster-server-0.test-cluster-nodes.test-namespace.svc:15672"))
			})

			It("returns the correct base URL for TLS with pod DNS name", func() {
				rmq.Spec.TLS.SecretName = "tls-secret"
				rmq.Spec.TLS.DisableNonTLSListeners = true
				podName := "test-cluster-server-1"
				info, err := getClientInfoForPod(ctx, k8sClient, rmq, podName)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.BaseURL).To(Equal("https://test-cluster-server-1.test-cluster-nodes.test-namespace.svc:15671"))
			})

			It("returns an HTTP transport for TLS with correct ServerName", func() {
				rmq.Spec.TLS.SecretName = "tls-secret"
				rmq.Spec.TLS.DisableNonTLSListeners = true
				podName := "test-cluster-server-0"
				info, err := getClientInfoForPod(ctx, k8sClient, rmq, podName)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.Transport).NotTo(BeNil())
				Expect(info.Transport.TLSClientConfig).NotTo(BeNil())
				Expect(info.Transport.TLSClientConfig.ServerName).To(Equal("test-cluster-server-0.test-cluster-nodes.test-namespace.svc"))
			})

			It("returns nil transport for non-TLS", func() {
				podName := "test-cluster-server-0"
				info, err := getClientInfoForPod(ctx, k8sClient, rmq, podName)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.Transport).To(BeNil())
			})
		})

		Context("when the secret does not exist", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq).
					Build()
			})

			It("returns an error", func() {
				podName := "test-cluster-server-0"
				_, err := getClientInfoForPod(ctx, k8sClient, rmq, podName)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get default user secret"))
			})
		})
	})

	Describe("GetClientInfoForService", func() {
		var rabbitmqService *corev1.Service

		BeforeEach(func() {
			rabbitmqService = &corev1.Service{
				Name:      "test-cluster",
				Namespace: namespace,
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "management", Port: 15672, Protocol: corev1.ProtocolTCP},
					},
				},
			}
		})

		When("the secret and service exist", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, secret, rabbitmqService).
					Build()
			})

			It("returns client info with correct credentials", func() {
				info, err := getClientInfoForService(ctx, k8sClient, rmq)
				Expect(err).NotTo(HaveOccurred())
				Expect(info).NotTo(BeNil())
				Expect(info.Username).To(Equal("test-user"))
				Expect(info.Password).To(Equal("test-password"))
			})

			It("returns the correct base URL for non-TLS using the main Service DNS name", func() {
				info, err := getClientInfoForService(ctx, k8sClient, rmq)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.BaseURL).To(Equal("http://test-cluster.test-namespace.svc:15672"))
			})

			It("returns the correct base URL for TLS using the main Service DNS name", func() {
				rmq.Spec.TLS.SecretName = "tls-secret"
				rmq.Spec.TLS.DisableNonTLSListeners = true
				rabbitmqService.Spec.Ports = []corev1.ServicePort{
					{Name: "management-tls", Port: 15671, Protocol: corev1.ProtocolTCP},
				}
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, secret, rabbitmqService).
					Build()
				info, err := getClientInfoForService(ctx, k8sClient, rmq)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.BaseURL).To(Equal("https://test-cluster.test-namespace.svc:15671"))
			})

			It("returns an HTTP transport for TLS with correct ServerName", func() {
				rmq.Spec.TLS.SecretName = "tls-secret"
				rmq.Spec.TLS.DisableNonTLSListeners = true
				rabbitmqService.Spec.Ports = []corev1.ServicePort{
					{Name: "management-tls", Port: 15671, Protocol: corev1.ProtocolTCP},
				}
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, secret, rabbitmqService).
					Build()
				info, err := getClientInfoForService(ctx, k8sClient, rmq)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.Transport).NotTo(BeNil())
				Expect(info.Transport.TLSClientConfig).NotTo(BeNil())
				Expect(info.Transport.TLSClientConfig.ServerName).To(Equal("test-cluster.test-namespace.svc"))
			})

			It("returns nil transport for non-TLS", func() {
				info, err := getClientInfoForService(ctx, k8sClient, rmq)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.Transport).To(BeNil())
			})
		})

		When("the service does not exist", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, secret).
					Build()
			})

			It("returns an error", func() {
				_, err := getClientInfoForService(ctx, k8sClient, rmq)
				Ω(err).Should(MatchError(ContainSubstring("failed to get rabbitmq service")))
			})
		})

		When("the secret does not exist", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, rabbitmqService).
					Build()
			})

			It("returns an error", func() {
				_, err := getClientInfoForService(ctx, k8sClient, rmq)
				Ω(err).Should(MatchError(ContainSubstring("failed to get default user secret")))
			})
		})
	})

	Describe("DefaultRabbitmqClientFactory", func() {
		var factory RabbitmqClientFactory
		var rabbitmqService *corev1.Service

		BeforeEach(func() {
			factory = &DefaultRabbitmqClientFactory{}
			rabbitmqService = &corev1.Service{
				Name:      "test-cluster",
				Namespace: namespace,
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "management", Port: 15672, Protocol: corev1.ProtocolTCP},
						{Name: "management-tls", Port: 15671, Protocol: corev1.ProtocolTCP},
					},
				},
			}
			k8sClient = fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(rmq, secret, rabbitmqService).
				Build()
		})

		DescribeTable("uses the management path prefix for Pod and Service API requests",
			func(additionalConfig, expectedPrefix string, tlsOnly bool) {
				rmq.Spec.Rabbitmq.AdditionalConfig = additionalConfig
				rmq.Spec.TLS.DisableNonTLSListeners = tlsOnly
				for _, target := range []string{"pod", "service"} {
					var managementClient RabbitmqClient
					var err error
					if target == "pod" {
						managementClient, err = factory.GetClientForPod(ctx, k8sClient, rmq, "test-cluster-server-0")
					} else {
						managementClient, err = factory.GetClientForService(ctx, k8sClient, rmq)
					}
					Expect(err).NotTo(HaveOccurred())

					var paths []string
					managementClient.(*rabbithole.Client).SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
						paths = append(paths, req.URL.Path)
						if tlsOnly {
							Expect(req.URL.Scheme).To(Equal("https"))
						} else {
							Expect(req.URL.Scheme).To(Equal("http"))
						}
						body := `{"status":"ok"}`
						if strings.HasSuffix(req.URL.Path, "/deprecated-features/used") {
							body = "[]"
						}
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     make(http.Header),
							Body:       io.NopCloser(strings.NewReader(body)),
							Request:    req,
						}, nil
					}))

					_, err = managementClient.Overview()
					Expect(err).NotTo(HaveOccurred())
					_, err = managementClient.HealthCheckNodeIsQuorumCritical()
					Expect(err).NotTo(HaveOccurred())
					_, err = managementClient.ListDeprecatedFeaturesUsed()
					Expect(err).NotTo(HaveOccurred())
					Expect(paths).To(Equal([]string{
						expectedPrefix + "/api/overview",
						expectedPrefix + "/api/health/checks/node-is-quorum-critical",
						expectedPrefix + "/api/deprecated-features/used",
					}))
				}
			},
			Entry("no prefix", "", "", false),
			Entry("configured prefix", "management.path_prefix = /foo", "/foo", false),
			Entry("leading and trailing slashes", "management.path_prefix = //foo//", "/foo", false),
			Entry("root prefix", "management.path_prefix = /", "", false),
			Entry("prefix among other settings", "some_key = 1\nmanagement.path_prefix = /foo\nother_key = 2", "/foo", false),
			Entry("TLS prefix", "management.path_prefix = /foo", "/foo", true),
		)

		It("returns configuration parsing errors for both clients", func() {
			rmq.Spec.Rabbitmq.AdditionalConfig = " = invalid"
			_, err := factory.GetClientForPod(ctx, k8sClient, rmq, "test-cluster-server-0")
			Expect(err).To(MatchError(ContainSubstring("failed to load spec.rabbitmq.additionalConfig")))
			_, err = factory.GetClientForService(ctx, k8sClient, rmq)
			Expect(err).To(MatchError(ContainSubstring("failed to load spec.rabbitmq.additionalConfig")))
		})

		When("TLS is disabled", func() {
			It("returns a non-TLS client for pod", func() {
				podName := "test-cluster-server-0"
				client, err := factory.GetClientForPod(ctx, k8sClient, rmq, podName)
				Expect(err).NotTo(HaveOccurred())
				Expect(client).NotTo(BeNil())
			})

			It("returns a non-TLS client for service", func() {
				client, err := factory.GetClientForService(ctx, k8sClient, rmq)
				Expect(err).NotTo(HaveOccurred())
				Expect(client).NotTo(BeNil())
			})
		})

		When("TLS is enabled", func() {
			It("returns a TLS client for pod", func() {
				rmq.Spec.TLS.SecretName = "tls-secret"
				rmq.Spec.TLS.DisableNonTLSListeners = true
				podName := "test-cluster-server-0"
				client, err := factory.GetClientForPod(ctx, k8sClient, rmq, podName)
				Expect(err).NotTo(HaveOccurred())
				Expect(client).NotTo(BeNil())
			})

			It("returns a TLS client for service", func() {
				rmq.Spec.TLS.SecretName = "tls-secret"
				rmq.Spec.TLS.DisableNonTLSListeners = true
				client, err := factory.GetClientForService(ctx, k8sClient, rmq)
				Expect(err).NotTo(HaveOccurred())
				Expect(client).NotTo(BeNil())
			})
		})

		When("the secret does not exist", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, rabbitmqService).
					Build()
			})

			It("returns an error for pod", func() {
				podName := "test-cluster-server-0"
				_, err := factory.GetClientForPod(ctx, k8sClient, rmq, podName)
				Ω(err).Should(MatchError(ContainSubstring("failed to get default user secret")))
			})

			It("returns an error for service", func() {
				_, err := factory.GetClientForService(ctx, k8sClient, rmq)
				Ω(err).Should(MatchError(ContainSubstring("failed to get default user secret")))
			})
		})

		When("the service does not exist", func() {
			BeforeEach(func() {
				k8sClient = fake.NewClientBuilder().
					WithScheme(scheme).
					WithObjects(rmq, secret).
					Build()
			})

			It("returns an error for service client", func() {
				_, err := factory.GetClientForService(ctx, k8sClient, rmq)
				Ω(err).Should(MatchError(ContainSubstring("failed to get rabbitmq service")))
			})
		})
	})
})
