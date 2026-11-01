package mqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hookdeck/outpost/internal/mqs"
	"github.com/hookdeck/outpost/internal/util/testinfra"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMQ_NATSUnreachableServerFailsFast(t *testing.T) {
	t.Parallel()
	// Nothing listens on this port, so the connect attempt inside Publish
	// must surface an error rather than hang or panic.
	queue := mqs.NewNATSQueue(&mqs.NATSConfig{
		ServerURL: "nats://127.0.0.1:1",
		Subject:   "test",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := queue.Publish(ctx, &Msg{ID: "first"})
	require.Error(t, err)
}

func TestIntegrationMQ_NATSLoggableIDPerMessage(t *testing.T) {
	t.Parallel()
	t.Cleanup(testinfra.Start(t))
	config := testinfra.NewMQNATSConfig(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	queue := mqs.NewQueue(&config)
	cleanup, err := queue.Init(ctx)
	require.NoError(t, err)
	defer cleanup()

	subscription, err := queue.Subscribe(ctx)
	require.NoError(t, err)
	defer subscription.Shutdown(ctx)

	require.NoError(t, queue.Publish(ctx, &Msg{ID: "first"}))
	require.NoError(t, queue.Publish(ctx, &Msg{ID: "second"}))

	first, err := subscription.Receive(ctx)
	require.NoError(t, err)
	first.Ack()
	second, err := subscription.Receive(ctx)
	require.NoError(t, err)
	second.Ack()

	assert.NotEmpty(t, first.LoggableID)
	assert.NotEqual(t, first.LoggableID, second.LoggableID)
}

func TestIntegrationMQ_NATSPublishQueueConsumesExistingStream(t *testing.T) {
	t.Parallel()
	t.Cleanup(testinfra.Start(t))
	serverURL := testinfra.EnsureNATS(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc, err := nats.Connect(serverURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)

	streamName := "publish-" + uuid.NewString()[:8]
	subject := streamName + ".events"
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subject},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.MemoryStorage,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, js.DeleteStream(context.Background(), streamName))
	}()

	queue := mqs.NewNATSPublishQueue(&mqs.NATSPublishConfig{
		ServerURL:    serverURL,
		Stream:       streamName,
		Subject:      subject,
		ConsumerName: "outpost-publish-test",
	})
	subscription, err := queue.Subscribe(ctx)
	require.NoError(t, err)
	defer subscription.Shutdown(ctx)

	body := []byte(`{"tenant_id":"tenant","topic":"user.created","data":{"id":"1"}}`)
	_, err = js.Publish(ctx, subject, body)
	require.NoError(t, err)
	msg, err := subscription.Receive(ctx)
	require.NoError(t, err)
	assert.Equal(t, body, msg.Body)
	assert.NotEmpty(t, msg.ID)
	msg.Ack()

	stream, err := js.Stream(ctx, streamName)
	require.NoError(t, err)
	assert.Equal(t, []string{subject}, stream.CachedInfo().Config.Subjects)
	consumer, err := stream.Consumer(ctx, "outpost-publish-test")
	require.NoError(t, err)
	assert.Equal(t, -1, consumer.CachedInfo().Config.MaxDeliver)
}
