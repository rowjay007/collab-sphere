package worker

import "context"

func (g *GC) Pass(ctx context.Context) { g.pass(ctx) }
