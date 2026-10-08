# Reading llm-d's Go, as a Python programmer

You write Python here; each day's map sends you into llm-d-router's Go.
You only need to read it. Ten things cover almost every line you will meet.

| Go | Python reading |
|---|---|
| `func (s *QueueScorer) Score(ctx context.Context, req *Req, eps []Endpoint) map[Endpoint]float64` | a method `QueueScorer.score(self, req, eps) -> dict[Endpoint, float]`. The receiver `(s *QueueScorer)` is `self`; `ctx` carries deadlines and loggers, ignore it |
| `type Scorer interface { Score(...) ... }` | a `typing.Protocol`. Any type with those methods satisfies it, with no `class X(Scorer)` needed |
| `var _ fwksched.Scorer = &QueueScorer{}` | a compile-time check that `QueueScorer` satisfies the interface; no runtime effect |
| `x, err := f(); if err != nil { return nil, err }` | `x = f()`, letting the exception propagate. Go returns errors as values; most of those lines are this |
| `for _, e := range endpoints {` | `for e in endpoints:` (`_` is the index you do not need) |
| `m := make(map[string]int)`, `m[k]++` | `m = {}`, `m[k] = m.get(k, 0) + 1`; a missing key reads as zero |
| `defer mu.Unlock()` | the `finally:` of a `with lock:` block; runs when the function returns |
| `go func() { ... }()` | start a thread (`threading.Thread(target=..., daemon=True).start()`) |
| `ch <- v`, `v := <-ch`, `select { case ...: }` | `queue.put(v)`, `queue.get()`, waiting on whichever of several queues is ready first |
| `logger.V(logutil.DEBUG).Info(...)`, `span.SetAttributes(...)`, `metrics.Record...` | logging, tracing, metrics. Skip them on a first read |

Also:

- **Capitalised names are exported** (`Schedule`, public); lowercase ones
  are package-private (`runScorer`), like a leading `_`.
- **`*T` is a pointer.** For reading, treat `*Endpoint` and `Endpoint` as the
  same object. `&T{...}` constructs one.
- **Struct tags** like `` `json:"maxNumOfEndpoints"` `` name the field in
  config files; that is where plugin parameters come from.
- **`_test.go` files** sit next to the code. They are often the fastest way
  to see what a function is supposed to do.

A good habit for each file in a map: find the function the map names, skip
every logging and tracing line, and rewrite what is left as five lines of
Python in your notes. Then compare those with your own code.
