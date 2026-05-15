import { z } from 'zod';

const channelEnum = z.enum(['EMAIL', 'SMS', 'PUSH']);

const priorityEnum = z.enum(['CRITICAL', 'HIGH', 'TRANSACTIONAL', 'MARKETING', 'LOW']);

const basePayload = z
  .object({
    channels: z.array(channelEnum).min(1).optional(),
    email: z.string().email().optional(),
    phone: z.string().min(8).max(32).optional(),
    deviceToken: z.string().min(10).max(512).optional(),
  })
  .passthrough();

export const eventBodySchema = z.object({
  eventId: z.string().uuid().optional(),
  type: z.string().min(1),
  source: z.enum(['order', 'payment', 'shipping', 'other']).optional(),
  priority: priorityEnum.optional().default('LOW'),
  payload: z.preprocess(
    (v) => (v && typeof v === 'object' ? v : {}),
    basePayload,
  ),
}).superRefine((data, ctx) => {
  const p = data.payload;
  const channels = p.channels?.length ? [...new Set(p.channels)] : ['EMAIL', 'SMS'];
  if (channels.includes('EMAIL') && !p.email) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'payload.email is required when EMAIL is selected (explicitly or by default channels)', path: ['payload', 'email'] });
  }
  if (channels.includes('SMS') && !p.phone) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'payload.phone is required when SMS is selected (explicitly or by default channels)', path: ['payload', 'phone'] });
  }
  if (channels.includes('PUSH') && !p.deviceToken) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'payload.deviceToken is required when PUSH is selected', path: ['payload', 'deviceToken'] });
  }
});

export { channelEnum, priorityEnum };
