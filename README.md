# eventually-consistent

Code and reproducible benchmarks behind the articles on [ggasg.github.io](https://ggasg.github.io).

The name is a small joke at this repo's own expense. Distributed systems trade
strict consistency for availability and call it a design decision. This repo
trades a predictable publishing schedule for the same, articles show up when
they show up, but by the time they land here, the numbers have converged: the
code ran, the tests passed, and the results are what they say they are.

One self-contained folder per article. Each folder has its own README, code,
tests, and results, so it can be cloned and run on its own without pulling in
anything else from this repo.

## Articles

| Article | Folder | Published |
|---|---|---|
| One Join, Two APIs, Zero Difference: Scala Datasets vs PySpark DataFrames on the JVM | [`spark-join-parity/`](./spark-join-parity) | *(add link once live)* |

## Adding a new article

1. Create a new top-level folder, named after the article's slug (e.g. `spark-join-parity`).
2. Inside it: `article.md`, `code/`, `results/` (if there's measured data), `tests/`.
3. Give the folder its own `README.md` (how to reproduce, what's inside). Copy `spark-join-parity/README.md` as a starting template.
4. Add a row to the table above.

## Conventions

- Every article with empirical claims ships the code that produced the
  numbers, not just the numbers.
- Anything with real logic (parsers, benchmark harnesses, data generators)
  gets a test, however small.
- A folder only graduates to its own repo if it turns into something
  reusable on its own (a library, a CLI tool). Otherwise it stays here.
