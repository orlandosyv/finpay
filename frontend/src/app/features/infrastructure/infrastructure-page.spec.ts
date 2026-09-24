import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { InfrastructurePage } from './infrastructure-page';

describe('InfrastructurePage', () => {
  let http: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [InfrastructurePage],
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    http = TestBed.inject(HttpTestingController);
  });

  afterEach(() => http.verify());

  it('separates live Kafka evidence from the educational architecture view', () => {
    const fixture = TestBed.createComponent(InfrastructurePage);
    fixture.detectChanges();

    http.expectOne('/api/merchant/infrastructure').flush({
      checkedAt: '2026-09-22T20:00:00Z',
      kafka: {
        status: 'CONNECTED',
        topic: 'finpay.payment-events.v1',
        topicAvailable: true,
        partitions: 3,
        publishingEnabled: true,
        clusterId: 'finpay-cluster',
        totalEvents: 3,
        pendingEvents: 1,
        publishedEvents: 2,
        failedEvents: 0,
        recentEvents: [
          {
            eventId: 'event-1',
            paymentId: 91,
            eventType: 'payment.created',
            status: 'PUBLISHED',
            attempts: 1,
            partition: 2,
            offset: 14,
            createdAt: '2026-09-22T20:00:00Z',
            publishedAt: '2026-09-22T20:00:01Z',
            nextAttemptAt: null,
            lastError: null,
          },
        ],
        consumer: {
          checkedAt: '2026-09-22T20:00:02Z',
          status: 'RUNNING',
          application: 'finpay-kafka-listener',
          topic: 'finpay.payment-events.v1',
          consumerGroup: 'finpay-audit-v1',
          concurrency: 3,
          consumedEvents: 2,
          consumerGroupLag: 0,
          lastConsumedAt: '2026-09-22T20:00:02Z',
          recentEvents: [
            {
              eventId: 'event-1',
              paymentId: 91,
              eventType: 'payment.created',
              paymentStatus: 'PENDING',
              partition: 2,
              offset: 14,
              occurredAt: '2026-09-22T20:00:00Z',
              consumedAt: '2026-09-22T20:00:02Z',
            },
          ],
          deadLetter: {
            topic: 'finpay.payment-events.v1.DLT',
            maxRetries: 3,
            retryIntervalMs: 2000,
            events: 1,
            recentEvents: [
              {
                eventId: 'event-invalid',
                originalTopic: 'finpay.payment-events.v1',
                originalPartition: 1,
                originalOffset: 15,
                exceptionClass: 'java.lang.IllegalArgumentException',
                exceptionMessage: 'Payment event payload is invalid',
                failedAt: '2026-09-22T20:00:03Z',
              },
            ],
          },
          hint: 'The listener is caught up.',
        },
        hint: 'The publisher is active.',
      },
      webhooks: {
        status: 'ACTIVE',
        activeEndpoints: 1,
        deliveryMode: 'SIGNED_HTTP_WITH_RETRIES',
        hint: 'Payment events can be delivered.',
      },
      pipeline: [
        {
          order: 1,
          name: 'Payment transaction',
          technology: 'SQL Server',
          state: 'ACTIVE',
          hint: 'Source of truth.',
        },
        {
          order: 4,
          name: 'Internal event stream',
          technology: 'Apache Kafka',
          state: 'ACTIVE',
          hint: 'Events are published.',
        },
        {
          order: 5,
          name: 'Independent audit projection',
          technology: 'finpay-kafka-listener',
          state: 'RUNNING',
          hint: 'The listener is caught up.',
        },
      ],
    });
    fixture.detectChanges();

    let text = fixture.nativeElement.textContent;
    expect(text).toContain('CONNECTED');
    expect(text).toContain('finpay.payment-events.v1');
    expect(text).toContain('Active');
    expect(text).toContain('payment.created');
    expect(text).toContain('P2 · O14');
    expect(text).toContain('finpay-kafka-listener');
    expect(text).toContain('finpay-audit-v1');
    expect(text).toContain('Consumer lag');
    expect(text).toContain('Recently consumed events');
    expect(text).toContain('Producer-side evidence');
    expect(text).toContain('Dead Letter Topic');
    expect(text).toContain('Payment event payload is invalid');
    expect(text).toContain('3 retries');
    expect(text).not.toContain('Transactional outbox');

    const learnTab: HTMLButtonElement = fixture.nativeElement.querySelector('#learn-tab');
    learnTab.click();
    fixture.detectChanges();

    text = fixture.nativeElement.textContent;
    expect(text).toContain('Transactional outbox');
    expect(text).toContain('With Kafka');
    expect(text).toContain('Without Kafka');
    expect(text).toContain('Stop the listener');
    expect(text).toContain('Send an invalid Kafka record');
    expect(text).not.toContain('Recent Kafka publications');
  });
});
