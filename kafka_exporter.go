package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace      = "kafka"
	clientID       = "kafka_exporter"
	retentionBytes = "retention.bytes"
	retentionMs    = "retention.ms"
)

var (
	clusterBrokers                     *prometheus.Desc
	topicPartitions                    *prometheus.Desc
	topicCurrentOffset                 *prometheus.Desc
	topicOldestOffset                  *prometheus.Desc
	topicPartitionLeader               *prometheus.Desc
	topicPartitionReplicas             *prometheus.Desc
	topicPartitionInSyncReplicas       *prometheus.Desc
	topicPartitionUsesPreferredReplica *prometheus.Desc
	topicUnderReplicatedPartition      *prometheus.Desc
	topicRetentionBytes                *prometheus.Desc
	topicRetentionMs                   *prometheus.Desc
	consumergroupCurrentOffset         *prometheus.Desc
	consumergroupCurrentOffsetInstance *prometheus.Desc
	consumergroupCurrentOffsetSum      *prometheus.Desc
	consumergroupLag                   *prometheus.Desc
	consumergroupLagInstance           *prometheus.Desc
	consumergroupLagSum                *prometheus.Desc
	consumergroupLagZookeeper          *prometheus.Desc
	consumergroupMembers               *prometheus.Desc
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	adminClient             sarama.ClusterAdmin
	topicFilter             *regexp.Regexp
	groupFilter             *regexp.Regexp
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
}

type kafkaOpts struct {
	uri                      []string
	useSASL                  bool
	useSASLHandshake         bool
	saslUsername             string
	saslPassword             string
	useTLS                   bool
	tlsCAFile                string
	tlsCertFile              string
	tlsKeyFile               string
	tlsInsecureSkipTLSVerify bool
	kafkaVersion             string
	labels                   string
	metadataRefreshInterval  string
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts kafkaOpts, topicFilter string, groupFilter string) (*Exporter, error) {
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		return nil, err
	}
	config.Version = kafkaVersion

	if opts.useSASL {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if opts.saslPassword != "" {
			config.Net.SASL.Password = opts.saslPassword
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := ioutil.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				plog.Fatalln(err)
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			plog.Fatalln(err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				plog.Fatalln(err)
			}
		}
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		plog.Errorln("Cannot parse metadata refresh interval")
		panic(err)
	}

	config.Metadata.RefreshFrequency = interval

	client, err := sarama.NewClient(opts.uri, config)
	adminClient, err := sarama.NewClusterAdmin(opts.uri, config)

	if err != nil {
		plog.Errorln("Error Init Kafka Client")
		panic(err)
	}
	plog.Infoln("Done Init Clients")

	// Init our exporter.
	return &Exporter{
		client:                  client,
		adminClient:             adminClient,
		topicFilter:             regexp.MustCompile(topicFilter),
		groupFilter:             regexp.MustCompile(groupFilter),
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
	}, nil
}

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- clusterBrokers
	ch <- topicCurrentOffset
	ch <- topicOldestOffset
	ch <- topicPartitions
	ch <- topicPartitionLeader
	ch <- topicPartitionReplicas
	ch <- topicPartitionInSyncReplicas
	ch <- topicPartitionUsesPreferredReplica
	ch <- topicUnderReplicatedPartition
	ch <- topicRetentionBytes
	ch <- topicRetentionMs
	ch <- consumergroupCurrentOffset
	ch <- consumergroupCurrentOffsetInstance
	ch <- consumergroupCurrentOffsetSum
	ch <- consumergroupLag
	ch <- consumergroupLagInstance
	ch <- consumergroupLagZookeeper
	ch <- consumergroupLagSum
}

func (e *Exporter) Close() error {
	adminErr := e.adminClient.Close()
	clientErr := e.client.Close()

	if adminErr != nil {
		return adminErr
	}

	if clientErr != nil {
		return clientErr
	}

	return nil
}

func exportTopicDetail(ch chan<- prometheus.Metric, detail sarama.TopicDetail, desc *prometheus.Desc, topic, entry string) {
	if value := detail.ConfigEntries[entry]; value != nil {
		if result, err := strconv.ParseInt(*value, 10, 64); err == nil {
			ch <- prometheus.MustNewConstMetric(
				desc, prometheus.GaugeValue, float64(result), topic,
			)
		} else {
			plog.Errorf("Failed to parse topic value %v: %v = %v - %v", topic, entry, value, err)
		}
	}
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	var wg = sync.WaitGroup{}
	ch <- prometheus.MustNewConstMetric(
		clusterBrokers, prometheus.GaugeValue, float64(len(e.client.Brokers())),
	)

	offsetMutex := sync.RWMutex{}
	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		plog.Info("Refreshing client metadata")

		if err := e.client.RefreshMetadata(); err != nil {
			plog.Errorf("Cannot refresh topics, using cached data: %v", err)
		}

		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	allTopics, err := e.client.Topics()
	if err != nil {
		plog.Errorf("Cannot get topics: %v", err)
		return
	}

	topics := make([]string, 0, len(allTopics))

	for _, topic := range allTopics {
		if e.topicFilter.MatchString(topic) {
			topics = append(topics, topic)
		}
	}

	topicMetadata, err := e.adminClient.ListTopics()

	if err != nil {
		plog.Errorf("Cannot load topic admin info: %v", err)
	} else {
		for _, topic := range topics {
			if metadata, found := topicMetadata[topic]; found {
				exportTopicDetail(ch, metadata, topicRetentionBytes, topic, retentionBytes)
				exportTopicDetail(ch, metadata, topicRetentionMs, topic, retentionMs)
			}
		}
	}

	getTopicMetrics := func(topic string) {
		defer wg.Done()

		partitions, err := e.client.Partitions(topic)
		if err != nil {
			plog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
			return
		}

		ch <- prometheus.MustNewConstMetric(
			topicPartitions, prometheus.GaugeValue, float64(len(partitions)), topic,
		)

		offsetMutex.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		offsetMutex.Unlock()

		for _, partition := range partitions {
			broker, err := e.client.Leader(topic, partition)
			if err != nil {
				plog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionLeader, prometheus.GaugeValue, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			currentOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				plog.Errorf("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
			} else {
				func() {
					offsetMutex.Lock()
					defer offsetMutex.Unlock()

					offset[topic][partition] = currentOffset
					ch <- prometheus.MustNewConstMetric(
						topicCurrentOffset, prometheus.GaugeValue, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10),
					)
				}()
			}

			oldestOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				plog.Errorf("Cannot get oldest offset of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicOldestOffset, prometheus.GaugeValue, float64(oldestOffset), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			replicas, err := e.client.Replicas(topic, partition)
			if err != nil {
				plog.Errorf("Cannot get replicas of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionReplicas, prometheus.GaugeValue, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			inSyncReplicas, err := e.client.InSyncReplicas(topic, partition)
			if err != nil {
				plog.Errorf("Cannot get in-sync replicas of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionInSyncReplicas, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			if broker != nil && replicas != nil && len(replicas) > 0 && broker.ID() == replicas[0] {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
				)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			if replicas != nil && inSyncReplicas != nil && len(inSyncReplicas) < len(replicas) {
				ch <- prometheus.MustNewConstMetric(
					topicUnderReplicatedPartition, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
				)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicUnderReplicatedPartition, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
				)
			}
		}
	}

	for _, topic := range topics {
		wg.Add(1)
		go getTopicMetrics(topic)
	}

	getConsumerGroupMetrics := func() {
		groups, err := e.adminClient.ListConsumerGroups()
		if err != nil {
			plog.Errorf("Cannot get consumer group: %v", err)
			return
		}

		groupIds := make([]string, 0)
		for groupId := range groups {
			if e.groupFilter.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := e.adminClient.DescribeConsumerGroups(groupIds)
		if err != nil {
			plog.Errorf("Cannot get describe groups: %v", err)
			return
		}

		for _, group := range describeGroups {
			topicPartitions := make(map[string][]int32)

			for topic, partitions := range offset {
				topicPartitions[topic] = make([]int32, 0, len(partitions))
				for partition := range partitions {
					offsetMutex.RLock()
					topicPartitions[topic] = append(topicPartitions[topic], partition)
					offsetMutex.RUnlock()
				}
			}
			ch <- prometheus.MustNewConstMetric(
				consumergroupMembers, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
			)

			if offsetFetchResponse, err := e.adminClient.ListConsumerGroupOffsets(group.GroupId, topicPartitions); err != nil {
				plog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
			} else {
				for topic, partitions := range offsetFetchResponse.Blocks {
					// If the topic is not consumed by that consumer group, skip it
					topicConsumed := false
					for _, offsetFetchResponseBlock := range partitions {
						// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
						if offsetFetchResponseBlock.Offset != -1 {
							topicConsumed = true
							break
						}
					}
					if topicConsumed {
						var currentOffsetSum int64
						var lagSum int64
						for partition, offsetFetchResponseBlock := range partitions {

							err := offsetFetchResponseBlock.Err
							if err != sarama.ErrNoError {
								plog.Errorf("Error for  partition %d :%v", partition, err.Error())
								continue
							}

							consumerInstance := "undefined"

							for _, member := range group.Members {
								assignment, err := member.GetMemberAssignment()
								if err != nil {
									plog.Errorf("Error pulling group assignments %v:%v :%v", member.ClientId, member.ClientHost, err.Error())
								} else {
									for _, p := range assignment.Topics[topic] {
										if p == partition {
											consumerInstance = fmt.Sprintf("%v:%v", member.ClientId, member.ClientHost)
										}
									}
								}
							}

							currentOffset := offsetFetchResponseBlock.Offset
							currentOffsetSum += currentOffset
							ch <- prometheus.MustNewConstMetric(
								consumergroupCurrentOffset, prometheus.GaugeValue, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
							)
							ch <- prometheus.MustNewConstMetric(
								consumergroupCurrentOffsetInstance, prometheus.GaugeValue, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10), consumerInstance,
							)

							func() {
								offsetMutex.RLock()
								defer offsetMutex.RUnlock()
								if offset, ok := offset[topic][partition]; ok {
									// If the topic is consumed by that consumer group, but no offset associated with the partition
									// forcing lag to -1 to be able to alert on that
									var lag int64
									if offsetFetchResponseBlock.Offset == -1 {
										lag = -1
									} else {
										lag = offset - offsetFetchResponseBlock.Offset
										lagSum += lag
									}
									ch <- prometheus.MustNewConstMetric(
										consumergroupLag, prometheus.GaugeValue, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
									)
									ch <- prometheus.MustNewConstMetric(
										consumergroupLagInstance, prometheus.GaugeValue, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10), consumerInstance,
									)
								} else {
									plog.Errorf("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
								}
							}()
						}
						ch <- prometheus.MustNewConstMetric(
							consumergroupCurrentOffsetSum, prometheus.GaugeValue, float64(currentOffsetSum), group.GroupId, topic,
						)
						ch <- prometheus.MustNewConstMetric(
							consumergroupLagSum, prometheus.GaugeValue, float64(lagSum), group.GroupId, topic,
						)
					}
				}
			}
		}
	}

	getConsumerGroupMetrics()

	wg.Wait()
}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9308").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		topicFilter   = kingpin.Flag("topic.filter", "Regex that determines which topics to collect.").Default(".*").String()
		groupFilter   = kingpin.Flag("group.filter", "Regex that determines which consumer groups to collect.").Default(".*").String()
		logSarama     = kingpin.Flag("log.enable-sarama", "Turn on Sarama logging.").Default("false").Bool()

		opts = kafkaOpts{}
	)
	kingpin.Flag("kafka.server", "Address (host:port) of Kafka server.").Default("kafka:9092").StringsVar(&opts.uri)
	kingpin.Flag("sasl.enabled", "Connect using SASL/PLAIN.").Default("false").BoolVar(&opts.useSASL)
	kingpin.Flag("sasl.handshake", "Only set this to false if using a non-Kafka SASL proxy.").Default("true").BoolVar(&opts.useSASLHandshake)
	kingpin.Flag("sasl.username", "SASL user name.").Default("").StringVar(&opts.saslUsername)
	kingpin.Flag("sasl.password", "SASL user password.").Default("").StringVar(&opts.saslPassword)
	kingpin.Flag("tls.enabled", "Connect using TLS.").Default("false").BoolVar(&opts.useTLS)
	kingpin.Flag("tls.ca-file", "The optional certificate authority file for TLS client authentication.").Default("").StringVar(&opts.tlsCAFile)
	kingpin.Flag("tls.cert-file", "The optional certificate file for client authentication.").Default("").StringVar(&opts.tlsCertFile)
	kingpin.Flag("tls.key-file", "The optional key file for client authentication.").Default("").StringVar(&opts.tlsKeyFile)
	kingpin.Flag("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure.").Default("false").BoolVar(&opts.tlsInsecureSkipTLSVerify)
	kingpin.Flag("kafka.version", "Kafka broker version").Default(sarama.V1_0_0_0.String()).StringVar(&opts.kafkaVersion)
	kingpin.Flag("kafka.labels", "Kafka cluster name").Default("").StringVar(&opts.labels)
	kingpin.Flag("refresh.metadata", "Metadata refresh interval").Default("30s").StringVar(&opts.metadataRefreshInterval)

	plog.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	plog.Infoln("Starting kafka_exporter", version.Info())
	plog.Infoln("Build context", version.BuildContext())

	labels := make(map[string]string)

	// Protect against empty labels
	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	clusterBrokers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "brokers"),
		"Number of Brokers in the Kafka Cluster.",
		nil, labels,
	)
	topicPartitions = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partitions"),
		"Number of partitions for this Topic",
		[]string{"topic"}, labels,
	)
	topicCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_current_offset"),
		"Current Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)
	topicOldestOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_oldest_offset"),
		"Oldest Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionLeader = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader"),
		"Leader Broker ID of this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_replicas"),
		"Number of Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionInSyncReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_in_sync_replica"),
		"Number of In-Sync Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionUsesPreferredReplica = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader_is_preferred"),
		"1 if Topic/Partition is using the Preferred Broker",
		[]string{"topic", "partition"}, labels,
	)

	topicUnderReplicatedPartition = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_under_replicated_partition"),
		"1 if Topic/Partition is under Replicated",
		[]string{"topic", "partition"}, labels,
	)

	topicRetentionBytes = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "retention_bytes"),
		"Max bytes that will be kept for this topic",
		[]string{"topic"}, labels,
	)

	topicRetentionMs = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "retention_ms"),
		"Maximum time in ms that a message will be kept in this topic",
		[]string{"topic"}, labels,
	)

	consumergroupCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset"),
		"Current Offset of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupCurrentOffsetInstance = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset_instance"),
		"Current Offset of a ConsumerGroup at Topic/Partition/Consumer Instance",
		[]string{"consumergroup", "topic", "partition", "consumerinstance"}, labels,
	)

	consumergroupCurrentOffsetSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset_sum"),
		"Current Offset of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag"),
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupLagInstance = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag_instance"),
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition/Consumer Instance",
		[]string{"consumergroup", "topic", "partition", "consumerinstance"}, labels,
	)

	consumergroupLagZookeeper = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroupzookeeper", "lag_zookeeper"),
		"Current Approximate Lag(zookeeper) of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)

	consumergroupLagSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag_sum"),
		"Current Approximate Lag of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	if *logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, *topicFilter, *groupFilter)
	if err != nil {
		plog.Fatalln(err)
	}
	defer exporter.Close()
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + *metricsPath + `'>Metrics</a></p>
	        </body>
	        </html>`))
	})

	plog.Infoln("Listening on", *listenAddress)
	plog.Fatal(http.ListenAndServe(*listenAddress, nil))
}
