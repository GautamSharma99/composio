import ComposioClient from '@composio/client';
import { PusherClient, TChunkedTriggerData } from '../../types/pusher.types';
import { InternalService } from '../internal/InternalService';
import { SDKRealtimeCredentialsResponse } from '../internal/InternalService.types';
import {
  ComposioFailedToCreatePusherClientError,
  ComposioFailedToGetSDKRealtimeCredentialsError,
  ComposioFailedToSubscribeToPusherChannelError,
} from '../../errors/TriggerErrors';
import logger from '../../utils/logger';
import { telemetry } from '../../telemetry/Telemetry';

const MAX_CHUNK_INDEX = 1_000;
const CHUNK_TTL_MS = 60_000;
const MAX_PENDING_CHUNKED_EVENTS = 100;

type PendingChunkedEvent = {
  chunks: Map<number, string>;
  finalIndex?: number;
  timeout: ReturnType<typeof setTimeout>;
};

export class PusherService {
  // these values are set via the Apollo API `/internal/sdk/realtime/credentials` endpoint
  private clientId!: string;
  private pusherKey!: string;
  private pusherCluster!: string;
  private pusherChannel!: string;
  // these details are set via the client SDK
  private pusherBaseURL!: string;
  private apiKey!: string;
  private pusherClient!: PusherClient;
  private composioClient!: ComposioClient;
  private chunkCleanup?: () => void;

  constructor(client: ComposioClient) {
    this.composioClient = client;
    this.pusherBaseURL = client.baseURL;
    this.apiKey = client.apiKey ?? process.env.COMPOSIO_API_KEY ?? '';
    telemetry.instrument(this, 'PusherService');
  }

  /**
   * Creates a Pusher client
   *
   * This method is called when the Pusher client is first used.
   * It will fetch the SDK realtime credentials from the Apollo API and create a Pusher client.
   */
  private async getPusherClient() {
    if (!this.pusherClient) {
      const internalService = new InternalService(this.composioClient);
      let sdkRealtimeCredentials: SDKRealtimeCredentialsResponse;
      try {
        sdkRealtimeCredentials = await internalService.getSDKRealtimeCredentials();
      } catch (error) {
        throw new ComposioFailedToGetSDKRealtimeCredentialsError(
          'Failed to get SDK realtime credentials',
          {
            cause: error,
          }
        );
      }

      this.clientId = sdkRealtimeCredentials.projectId;
      this.pusherKey = sdkRealtimeCredentials.pusherKey;
      this.pusherCluster = sdkRealtimeCredentials.pusherCluster;
      this.pusherChannel = `private-${this.clientId}_triggers`;

      logger.debug(
        `[PusherService] Creating Pusher client for client ID: ${this.clientId} in cluster ${this.pusherCluster}`
      );

      // create the Pusher client
      try {
        const { default: Pusher } = await import('pusher-js');
        this.pusherClient = new Pusher(this.pusherKey, {
          cluster: this.pusherCluster,
          channelAuthorization: {
            endpoint: `${this.pusherBaseURL}/api/v3/internal/sdk/realtime/auth`,
            headers: {
              'x-api-key': this.apiKey,
            },
            transport: 'ajax',
          },
        });
      } catch (error) {
        throw new ComposioFailedToCreatePusherClientError('Failed to create Pusher client', {
          cause: error,
        });
      }
    }

    return this.pusherClient;
  }

  /**
   * Binds a chunked event to a Pusher client
   *
   *
   * @param channel - The Pusher client to bind the event to
   * @param event - The event to bind to
   * @param callback - The function to call when the event is received
   */
  private bindWithChunking(
    channel: PusherClient,
    event: string,
    callback: (data: Record<string, unknown>) => void
  ): () => void {
    try {
      channel.bind(event, callback);

      // Now the chunked variation. Allows arbitrarily long messages.
      const events = new Map<string, PendingChunkedEvent>();

      const deleteEvent = (id: string) => {
        const pending = events.get(id);
        if (pending) {
          clearTimeout(pending.timeout);
          events.delete(id);
        }
      };

      channel.bind('chunked-' + event, data => {
        let eventId: string | undefined;
        try {
          const typedData = data as TChunkedTriggerData;
          eventId = typedData?.id;

          // Validate chunked data
          if (
            !typedData ||
            typeof typedData.id !== 'string' ||
            typedData.id.length === 0 ||
            typeof typedData.index !== 'number' ||
            !Number.isInteger(typedData.index) ||
            typedData.index < 0 ||
            typedData.index > MAX_CHUNK_INDEX ||
            typeof typedData.chunk !== 'string' ||
            typeof typedData.final !== 'boolean'
          ) {
            throw new Error('Invalid chunked trigger data format');
          }

          if (!events.has(typedData.id)) {
            if (events.size >= MAX_PENDING_CHUNKED_EVENTS) {
              const oldestId = events.keys().next().value;
              if (oldestId !== undefined) deleteEvent(oldestId);
            }

            const timeout = setTimeout(() => {
              events.delete(typedData.id);
            }, CHUNK_TTL_MS);
            if (typeof timeout === 'object') timeout.unref?.();
            events.set(typedData.id, { chunks: new Map(), timeout });
          }

          const pending = events.get(typedData.id)!;
          pending.chunks.set(typedData.index, typedData.chunk);

          if (typedData.final) {
            if (pending.finalIndex !== undefined && pending.finalIndex !== typedData.index) {
              throw new Error('Conflicting final chunk indices');
            }
            pending.finalIndex = typedData.index;
          }

          const finalIndex = pending.finalIndex;
          if (finalIndex !== undefined) {
            if ([...pending.chunks.keys()].some(index => index > finalIndex)) {
              throw new Error('Chunk index exceeds the final chunk index');
            }

            const chunks = Array.from({ length: finalIndex + 1 }, (_, index) =>
              pending.chunks.get(index)
            );
            if (chunks.every((chunk): chunk is string => chunk !== undefined)) {
              try {
                const parsedData = JSON.parse(chunks.join(''));
                callback(parsedData);
              } catch (parseError: unknown) {
                const errorMessage =
                  parseError instanceof Error ? parseError.message : String(parseError);
                logger.error('Failed to parse chunked data:', errorMessage);
              } finally {
                deleteEvent(typedData.id);
              }
            }
          }
        } catch (error: unknown) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.error('Error processing chunked trigger data:', errorMessage);
          if (eventId !== undefined) deleteEvent(eventId);
        }
      });

      return () => {
        for (const id of events.keys()) deleteEvent(id);
      };
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('Failed to bind chunked events:', error);
      throw new Error(`Failed to bind chunked events: ${errorMessage}`);
    }
  }

  /**
   * Subscribes to pusher to receive events from the server
   *
   * This method is used to subscribe to a Pusher channel.
   * It will create a Pusher client if it doesn't exist.
   *
   * @param channelName - The name of the Pusher channel to subscribe to
   * @param event - The event to subscribe to
   * @param fn - The function to call when the event is received
   */
  async subscribe(fn: (data: Record<string, unknown>) => void) {
    try {
      logger.debug(`[PusherService] Subscribing to channel: ${this.pusherChannel}`);
      const pusherClient = await this.getPusherClient();
      const channel = await pusherClient.subscribe(this.pusherChannel);

      // add subscription error handling
      channel.bind('pusher:subscription_error', (data: Record<string, unknown>) => {
        const error = data.error ? String(data.error) : 'Unknown subscription error';
        throw new ComposioFailedToSubscribeToPusherChannelError(
          `Trigger subscription error: ${error}`,
          {
            cause: error,
          }
        );
      });

      // wrap the callback to handle errors
      const safeCallback = (data: Record<string, unknown>) => {
        try {
          fn(data);
        } catch (error: unknown) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.error('❌ Error in trigger callback:', errorMessage);
          // don't throw here to prevent breaking the subscription
        }
      };

      this.chunkCleanup?.();
      this.chunkCleanup = this.bindWithChunking(
        channel as PusherClient,
        'trigger_to_client',
        safeCallback
      );

      logger.info(`✅ Subscribed to triggers. You should start receiving events now.`);
    } catch (error) {
      throw new ComposioFailedToSubscribeToPusherChannelError(
        'Failed to subscribe to Pusher channel',
        {
          cause: error,
        }
      );
    }
  }

  /**
   * Unsubscribes from a Pusher channel
   *
   * This method is used to unsubscribe from a Pusher channel.
   * It will create a Pusher client if it doesn't exist.
   *
   * @param channelName - The name of the Pusher channel to unsubscribe from
   */
  async unsubscribe() {
    this.chunkCleanup?.();
    this.chunkCleanup = undefined;
    try {
      logger.debug(`[PusherService] Unsubscribing from channel: ${this.pusherChannel}`);
      const pusherClient = await this.getPusherClient();
      await pusherClient.unsubscribe(this.pusherChannel);
      logger.info(`✅ Unsubscribed from triggers.`);
    } catch (error) {
      throw new ComposioFailedToSubscribeToPusherChannelError(
        'Failed to unsubscribe from Pusher channel',
        {
          cause: error,
        }
      );
    }
  }
}
