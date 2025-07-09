package scalers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/go-logr/logr"
	v2 "k8s.io/api/autoscaling/v2"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type datadogQueries struct {
	input   string
	output  bool
	isError bool
}

type datadogScalerType int64

const (
	apiType datadogScalerType = iota
	clusterAgentType
)

type datadogMetricIdentifier struct {
	metadataTestData *datadogAuthMetadataTestData
	typeOfScaler     datadogScalerType
	triggerIndex     int
	name             string
}

type datadogAuthMetadataTestData struct {
	metricType v2.MetricTargetType
	metadata   map[string]string
	authParams map[string]string
	isError    bool
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a == b {
		return
	}
	t.Errorf("%v != %v", a, b)
}

func TestMaxFloatFromSlice(t *testing.T) {
	input := []float64{1.0, 2.0, 3.0, 4.0}
	expectedOutput := float64(4.0)

	output := slices.Max(input)

	assertEqual(t, output, expectedOutput)
}

func TestAvgFloatFromSlice(t *testing.T) {
	input := []float64{1.0, 2.0, 3.0, 4.0}
	expectedOutput := float64(2.5)

	output := AvgFloatFromSlice(input)

	assertEqual(t, output, expectedOutput)
}

var testParseQueries = []datadogQueries{
	{"", false, true},
	// All properly formed
	{"avg:system.cpu.user{*}.rollup(sum, 30)", true, false},
	{"sum:system.cpu.user{*}.rollup(30)", true, false},
	{"avg:system.cpu.user{automatic-restart:false,bosh_address:192.168.101.12}.rollup(avg, 30)", true, false},
	{"top(per_second(abs(sum:http.requests{service:myapp,dc:us-west-2}.rollup(max, 2))), 5, 'mean', 'desc')", true, false},
	{"system.cpu.user{*}.rollup(sum, 30)", true, false},
	{"min:system.cpu.user{*}", true, false},
	// Multi-query
	{"avg:system.cpu.user{*}.rollup(sum, 30),sum:system.cpu.user{*}.rollup(30)", true, false},

	// Missing filter
	{"min:system.cpu.user", false, true},

	// Find out last point with value
	{"sum:trace.express.request.hits{*}.as_rate()/avg:kubernetes.cpu.requests{*}.rollup(10)", true, false},
}

func TestDatadogScalerParseQueries(t *testing.T) {
	for _, testData := range testParseQueries {
		output, err := parseDatadogQuery(testData.input)

		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}

		if output != testData.output {
			t.Errorf("Expected %v, got %v", testData.output, output)
		}
	}
}

var testDatadogClusterAgentMetadata = []datadogAuthMetadataTestData{
	{"", map[string]string{}, map[string]string{}, true},

	// all properly formed
	{"", map[string]string{"useClusterAgentProxy": "true", "datadogMetricName": "nginx-hits", "datadogMetricNamespace": "default", "targetValue": "2", "type": "global"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "datadogMetricsServicePort": "8080", "unsafeSsl": "true", "authMode": "bearer"}, false},
	// Default Datadog service name and port
	{"", map[string]string{"useClusterAgentProxy": "true", "datadogMetricName": "nginx-hits", "datadogMetricNamespace": "default", "targetValue": "2", "type": "global"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "unsafeSsl": "true", "authMode": "bearer"}, false},

	// both metadata type and trigger type
	{v2.AverageValueMetricType, map[string]string{"useClusterAgentProxy": "true", "datadogMetricName": "nginx-hits", "datadogMetricNamespace": "default", "targetValue": "2", "type": "global"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "unsafeSsl": "true", "authMode": "bearer"}, true},
	// missing DatadogMetric name
	{"", map[string]string{"useClusterAgentProxy": "true", "datadogMetricNamespace": "default", "targetValue": "2", "type": "global"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "unsafeSsl": "true", "authMode": "bearer"}, true},
	// missing DatadogMetric namespace
	{"", map[string]string{"useClusterAgentProxy": "true", "datadogMetricName": "nginx-hits", "targetValue": "2", "type": "global"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "unsafeSsl": "true", "authMode": "bearer"}, true},
	// wrong port type
	{"", map[string]string{"useClusterAgentProxy": "true", "datadogMetricName": "nginx-hits", "datadogMetricNamespace": "default", "targetValue": "2", "type": "global"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "datadogMetricsServicePort": "notanint", "unsafeSsl": "true", "authMode": "bearer"}, true},
	// wrong targetValue type
	{"", map[string]string{"useClusterAgentProxy": "true", "datadogMetricName": "nginx-hits", "datadogMetricNamespace": "default", "targetValue": "notanint", "type": "global"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "datadogMetricsServicePort": "8080", "unsafeSsl": "true", "authMode": "bearer"}, true},
	// wrong type
	{"", map[string]string{"useClusterAgentProxy": "true", "datadogMetricName": "nginx-hits", "datadogMetricNamespace": "default", "targetValue": "2", "type": "notatype"}, map[string]string{"token": "token", "datadogNamespace": "datadog", "datadogMetricsService": "datadog-cluster-agent-metrics-api", "datadogMetricsServicePort": "8080", "unsafeSsl": "true", "authMode": "bearer"}, true},
}

var testDatadogAPIMetadata = []datadogAuthMetadataTestData{
	{"", map[string]string{}, map[string]string{}, true},

	// all properly formed
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "metricUnavailableValue": "1.5", "type": "average", "age": "60", "timeWindowOffset": "30", "lastAvailablePointOffset": "1"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, false},
	// Multi-query all properly formed
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count(),sum:trace.redis.command.hits{env:none,service:redis}.as_count()/2", "queryValue": "7", "queryAggregator": "average", "metricUnavailableValue": "1.5", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, false},
	// default age
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "type": "average"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, false},
	// default timeWindowOffset
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "metricUnavailableValue": "1.5", "type": "average", "age": "60", "lastAvailablePointOffset": "1"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, false},
	// default lastAvailablePointOffset
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "metricUnavailableValue": "1.5", "type": "average", "age": "60", "timeWindowOffset": "30"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, false},
	// default type
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, false},
	// wrong type
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "type": "invalid", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// both metadata type and trigger type
	{v2.AverageValueMetricType, map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// missing query
	{"", map[string]string{"queryValue": "7", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// missing queryValue
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// wrong query value type
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "notanint", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// wrong queryAggregator value
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "notanint", "queryAggegrator": "1.0", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// wrong activation query value type
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "1", "activationQueryValue": "notanint", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// malformed query
	{"", map[string]string{"query": "sum:trace.redis.command.hits", "queryValue": "7", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// wrong unavailableMetricValue type
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7", "metricUnavailableValue": "notafloat", "type": "average", "age": "60"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, true},
	// success api/app keys
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey", "datadogSite": "datadogSite"}, false},
	// default datadogSite
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7"}, map[string]string{"apiKey": "apiKey", "appKey": "appKey"}, false},
	// missing apiKey
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7"}, map[string]string{"appKey": "appKey"}, true},
	// missing appKey
	{"", map[string]string{"query": "sum:trace.redis.command.hits{env:none,service:redis}.as_count()", "queryValue": "7"}, map[string]string{"apiKey": "apiKey"}, true},
	// invalid query missing {
	{"", map[string]string{"query": "sum:trace.redis.command.hits.as_count()", "queryValue": "7"}, map[string]string{}, true},
}

func TestDatadogScalerAPIAuthParams(t *testing.T) {
	for _, testData := range testDatadogAPIMetadata {
		_, err := parseDatadogAPIMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata, AuthParams: testData.authParams, MetricType: testData.metricType}, logr.Discard())

		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}

func TestDatadogScalerClusterAgentAuthParams(t *testing.T) {
	for _, testData := range testDatadogClusterAgentMetadata {
		_, err := parseDatadogClusterAgentMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadata, AuthParams: testData.authParams, MetricType: testData.metricType}, logr.Discard())

		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}

var datadogMetricIdentifiers = []datadogMetricIdentifier{
	{&testDatadogAPIMetadata[1], apiType, 0, "s0-datadog-sum-trace-redis-command-hits"},
	{&testDatadogAPIMetadata[1], apiType, 1, "s1-datadog-sum-trace-redis-command-hits"},
	{&testDatadogClusterAgentMetadata[1], clusterAgentType, 0, "datadogmetric@default:nginx-hits"},
}

func TestDatadogGetMetricSpecForScaling(t *testing.T) {
	var err error
	var meta *datadogMetadata

	for _, testData := range datadogMetricIdentifiers {
		if testData.typeOfScaler == apiType {
			meta, err = parseDatadogAPIMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadataTestData.metadata, AuthParams: testData.metadataTestData.authParams, TriggerIndex: testData.triggerIndex, MetricType: testData.metadataTestData.metricType}, logr.Discard())
		} else {
			meta, err = parseDatadogClusterAgentMetadata(&scalersconfig.ScalerConfig{TriggerMetadata: testData.metadataTestData.metadata, AuthParams: testData.metadataTestData.authParams, TriggerIndex: testData.triggerIndex, MetricType: testData.metadataTestData.metricType}, logr.Discard())
		}
		if err != nil {
			t.Fatal("Could not parse metadata:", err)
		}

		mockDatadogScaler := datadogScaler{
			metadata:   meta,
			apiClient:  nil,
			httpClient: nil,
		}

		metricSpec := mockDatadogScaler.GetMetricSpecForScaling(context.Background())
		metricName := metricSpec[0].External.Metric.Name
		if metricName != testData.name {
			t.Error("Wrong External metric source name:", metricName)
		}
	}
}

func TestBuildClusterAgentURL(t *testing.T) {
	// Test valid inputs
	url := buildClusterAgentURL("datadogMetricsService", "datadogNamespace", 8080)
	if url != "https://datadogMetricsService.datadogNamespace:8080/apis/external.metrics.k8s.io/v1beta1" {
		t.Error("Expected https://datadogMetricsService.datadogNamespace:8080/apis/external.metrics.k8s.io/v1beta1, got ", url)
	}
}

func TestBuildMetricURL(t *testing.T) {
	// Test valid inputs
	url := buildMetricURL("https://localhost:8080/apis/datadoghq.com/v1alpha1", "datadogMetricNamespace", "datadogMetricName")
	if url != "https://localhost:8080/apis/datadoghq.com/v1alpha1/namespaces/datadogMetricNamespace/datadogMetricName" {
		t.Error("Expected https://localhost:8080/apis/datadoghq.com/v1alpha1/namespaces/datadogMetricNamespace/datadogMetricName, got ", url)
	}
}

func TestDatadogScalerGetMetricsAndActivity_ClusterAgent(t *testing.T) {
	testCases := []struct {
		name                  string
		responseCode          int
		responseBody          string
		activationTargetValue float64
		expectedMetricValue   float64
		expectedActivity      bool
		expectedError         bool
		expectedFallback      bool
	}{
		{
			name:                  "successful response",
			responseCode:          200,
			responseBody:          `{"items":[{"value":"4.0"}]}`,
			activationTargetValue: 1.0,
			expectedMetricValue:   4.0,
			expectedActivity:      true,
			expectedError:         false,
		},
		{
			name:                  "successful response below activation threshold",
			responseCode:          200,
			responseBody:          `{"items":[{"value":"0.5"}]}`,
			activationTargetValue: 1.0,
			expectedMetricValue:   0.5,
			expectedActivity:      false,
			expectedError:         false,
		},
		{
			name:                  "error response triggers fallback",
			responseCode:          500,
			responseBody:          `{"message":"Internal server error"}`,
			activationTargetValue: 1.0,
			expectedMetricValue:   2.5,
			expectedActivity:      true,
			expectedError:         false,
			expectedFallback:      true,
		},
		{
			name:                  "invalid JSON response triggers fallback",
			responseCode:          200,
			responseBody:          `invalid json`,
			activationTargetValue: 1.0,
			expectedMetricValue:   2.5,
			expectedActivity:      true,
			expectedError:         false,
			expectedFallback:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			metricName := "test-metric"

			// Create test server
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.responseCode)
				w.Write([]byte(tc.responseBody))
			}))
			defer testServer.Close()

			// Create metadata
			metadata := &datadogMetadata{
				fillValue:               2.5,
				useFiller:               true,
				activationTargetValue:   tc.activationTargetValue,
				hpaMetricName:           "test-hpa-metric",
				datadogMetricServiceURL: testServer.URL,
				datadogMetricNamespace:  "test-namespace",
				datadogMetricName:       "test-metric",
				enableBearerAuth:        true,
				bearerToken:             "test-token",
			}

			// Create scaler
			scaler := &datadogScaler{
				metadata:             metadata,
				logger:               logr.Discard(),
				useClusterAgentProxy: true,
				httpClient:           &http.Client{},
			}

			// Call GetMetricsAndActivity
			metrics, activity, err := scaler.GetMetricsAndActivity(ctx, metricName)

			// Verify results
			if tc.expectedError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tc.expectedError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if len(metrics) != 1 {
				t.Errorf("Expected 1 metric but got %d", len(metrics))
			}

			if len(metrics) > 0 {
				actualValue := metrics[0].Value.AsApproximateFloat64()
				if actualValue != tc.expectedMetricValue {
					t.Errorf("Expected metric value %f but got %f", tc.expectedMetricValue, actualValue)
				}
			}

			if activity != tc.expectedActivity {
				t.Errorf("Expected activity %v but got %v", tc.expectedActivity, activity)
			}
		})
	}
}

func TestDatadogScalerFallbackResponse(t *testing.T) {
	testCases := []struct {
		name                string
		useFiller           bool
		fillValue           float64
		expectedMetricValue float64
		expectedActivity    bool
	}{
		{
			name:                "fallback with filler enabled",
			useFiller:           true,
			fillValue:           2.5,
			expectedMetricValue: 2.5,
			expectedActivity:    true,
		},
		{
			name:                "fallback with filler disabled",
			useFiller:           false,
			fillValue:           3.0,
			expectedMetricValue: 3.0,
			expectedActivity:    true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &datadogMetadata{
				fillValue: tc.fillValue,
				useFiller: tc.useFiller,
			}

			scaler := &datadogScaler{
				metadata: metadata,
				logger:   logr.Discard(),
			}

			metricName := "test-metric"
			metrics, activity, err := scaler.fallbackResponse(metricName)

			if err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if len(metrics) != 1 {
				t.Errorf("Expected 1 metric but got %d", len(metrics))
			}

			if len(metrics) > 0 {
				actualValue := metrics[0].Value.AsApproximateFloat64()
				if actualValue != tc.expectedMetricValue {
					t.Errorf("Expected metric value %f but got %f", tc.expectedMetricValue, actualValue)
				}

				if metrics[0].MetricName != metricName {
					t.Errorf("Expected metric name %s but got %s", metricName, metrics[0].MetricName)
				}
			}

			if activity != tc.expectedActivity {
				t.Errorf("Expected activity %v but got %v", tc.expectedActivity, activity)
			}
		})
	}
}
