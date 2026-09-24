export interface KafkaInfrastructureStatus {
  status: 'CONNECTED' | 'DISCONNECTED' | 'DISABLED';
  topic: string;
  topicAvailable: boolean;
  partitions: number;
  publishingEnabled: boolean;
  clusterId: string | null;
  totalEvents: number;
  pendingEvents: number;
  publishedEvents: number;
  failedEvents: number;
  recentEvents: KafkaEventStatus[];
  consumer: KafkaConsumerStatus;
  hint: string;
}

export interface KafkaConsumerStatus {
  checkedAt: string;
  status: 'RUNNING' | 'STOPPED' | 'DISCONNECTED' | 'UNAVAILABLE' | 'DISABLED';
  application: string;
  topic: string | null;
  consumerGroup: string;
  concurrency: number;
  consumedEvents: number;
  consumerGroupLag: number;
  lastConsumedAt: string | null;
  recentEvents: KafkaConsumedEvent[];
  deadLetter: KafkaDeadLetterStatus;
  hint: string;
}

export interface KafkaDeadLetterStatus {
  topic: string;
  maxRetries: number;
  retryIntervalMs: number;
  events: number;
  recentEvents: KafkaDeadLetterEvent[];
}

export interface KafkaDeadLetterEvent {
  eventId: string | null;
  originalTopic: string;
  originalPartition: number;
  originalOffset: number;
  exceptionClass: string | null;
  exceptionMessage: string | null;
  failedAt: string;
}

export interface KafkaConsumedEvent {
  eventId: string;
  paymentId: number;
  eventType: string;
  paymentStatus: string;
  partition: number;
  offset: number;
  occurredAt: string;
  consumedAt: string;
}

export interface KafkaEventStatus {
  eventId: string;
  paymentId: number;
  eventType: string;
  status: 'PENDING' | 'PUBLISHED' | 'FAILED';
  attempts: number;
  partition: number | null;
  offset: number | null;
  createdAt: string;
  publishedAt: string | null;
  nextAttemptAt: string | null;
  lastError: string | null;
}

export interface WebhookInfrastructureStatus {
  status: 'ACTIVE' | 'NOT_CONFIGURED';
  activeEndpoints: number;
  deliveryMode: string;
  hint: string;
}

export interface InfrastructurePipelineStage {
  order: number;
  name: string;
  technology: string;
  state: string;
  hint: string;
}

export interface InfrastructureStatusResponse {
  checkedAt: string;
  kafka: KafkaInfrastructureStatus;
  webhooks: WebhookInfrastructureStatus;
  pipeline: InfrastructurePipelineStage[];
}
