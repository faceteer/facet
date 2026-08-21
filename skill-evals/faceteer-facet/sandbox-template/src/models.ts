import { z } from 'zod';
import type { PickValidator, Validator } from '@faceteer/facet';

export const ticketStatuses = ['open', 'pending', 'closed'] as const;
export type TicketStatus = (typeof ticketStatuses)[number];

export const ticketSchema = z.object({
	orgId: z.string(),
	ticketId: z.string(),
	subject: z.string(),
	status: z.enum(ticketStatuses),
	priority: z.number().int().min(1).max(5),
	assigneeId: z.string().optional(),
	createdAt: z.coerce.date(),
	dueAt: z.coerce.date(),
});
export type Ticket = z.infer<typeof ticketSchema>;

export const ticketValidator: Validator<Ticket> = (input) =>
	ticketSchema.parse(input);

export const ticketPickValidator: PickValidator<Ticket> = (keys) => {
	const mask: Record<string, true> = {};
	for (const key of keys) mask[key as string] = true;
	const picked = ticketSchema.pick(mask as never);
	return (input) => picked.parse(input) as never;
};

export const watcherSchema = z.object({
	orgId: z.string(),
	watcherId: z.string(),
	email: z.string(),
	createdAt: z.coerce.date(),
});
export type Watcher = z.infer<typeof watcherSchema>;

export const watcherValidator: Validator<Watcher> = (input) =>
	watcherSchema.parse(input);
