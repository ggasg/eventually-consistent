# eventually-consistent

Code and tests behind the articles on [ggasg.github.io](https://ggasg.github.io).

The name is a small joke at this repo's own expense. Distributed systems trade
strict consistency for availability and call it a design decision. This repo
trades a predictable publishing schedule for the same, articles show up when
they show up, but by the time they land here they have converged: the code ran,
the tests passed, and nothing claims more than it can show.

One self-contained folder per article. Each folder has its own README, code and
tests, so it can be cloned and run on its own without pulling in anything else
from this repo.

## Articles

| Article | Folder | Published |
|---|---|---|
| Scala Datasets vs PySpark DataFrames on the JVM (in progress: the sequential-session benchmark didn't hold up under scrutiny; redoing on Databricks Runtime 18 versions on real hardware) | [`spark-join-parity/`](./spark-join-parity) | not published |
| Counting failing devices without keeping their names | [`counting-failing-devices/`](./counting-failing-devices) | not published |

## Adding a new article

1. Create a new top-level folder, named after the article's slug (e.g. `spark-join-parity`).
2. Inside it: `code/`, `tests/`, and `results/` if there is measured data.
3. Give the folder its own `README.md` (how to reproduce, what's inside). Copy `spark-join-parity/README.md` as a starting template.
4. Add a row to the table above.

## Conventions

- Anything with real logic (parsers, generators, anything with branching)
  gets a test, however small.
- A folder only graduates to its own repo if it turns into something
  reusable on its own (a library, a CLI tool). Otherwise it stays here.
