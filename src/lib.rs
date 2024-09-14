//! Async functions

use std::future::Future;

use tokio::task::{JoinError, JoinSet};

macro_rules! spawnk {
    ($input:expr, $concurrency:expr, $f:expr) => {{
        assert!($concurrency > 0);
        let mut tasks = JoinSet::new();
        let mut input = $input.into_iter();
        for _ in 0..$concurrency {
            if let Some(t) = input.next() {
                let task = $f(t);
                tasks.spawn(task);
            } else {
                break;
            }
        }
        (tasks, input)
    }};
}

macro_rules! spawn {
    ($tasks:expr, $input:expr, $f:expr) => {
        if let Some(t) = $input.next() {
            let task = $f(t);
            $tasks.spawn(task);
        }
    };
}

/// Run tasks concurrently with specified `concurrency`
pub async fn cont<I, T, U, F, Fut>(input: I, concurrency: usize, f: F) -> Vec<U>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
{
    let input = input.into_iter().enumerate();
    let newf = move |(i, t)| {
        let u = f(t);
        async move {
            let x = u.await;
            (i, x)
        }
    };
    let mut ret = cont_unordered(input, concurrency, newf).await;
    ret.sort_by(|x, y| x.0.cmp(&y.0));
    ret.into_iter().map(|x| x.1).collect()
}

/// Run tasks concurrently with specified `concurrency`, the results are **not** ordered
pub async fn cont_unordered<I, T, U, F, Fut>(input: I, concurrency: usize, f: F) -> Vec<U>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = U> + Send + 'static,
    U: Send + 'static,
{
    let (mut tasks, mut input) = spawnk!(input, concurrency, f);
    let mut ret = vec![];
    while let Some(res) = tasks.join_next().await {
        let d = unpack_join_result(res).await;
        ret.push(d);
        spawn!(tasks, input, f);
    }
    ret
}

/// Run tasks concurrently with specified `concurrency`
pub async fn try_cont<I, T, U, E, F, Fut>(input: I, concurrency: usize, f: F) -> Result<Vec<U>, E>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<U, E>> + Send + 'static,
    U: Send + 'static,
    E: Send + 'static,
{
    let input = input.into_iter().enumerate();
    let newf = move |(i, t)| {
        let u = f(t);
        async move {
            let x = u.await?;
            Ok((i, x))
        }
    };
    let mut ret = try_cont_unordered(input, concurrency, newf).await?;
    ret.sort_by(|x, y| x.0.cmp(&y.0));
    Ok(ret.into_iter().map(|x| x.1).collect())
}

/// Run tasks concurrently with specified `concurrency`, the results are **not** ordered
pub async fn try_cont_unordered<I, T, U, E, F, Fut>(
    input: I,
    concurrency: usize,
    f: F,
) -> Result<Vec<U>, E>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<U, E>> + Send + 'static,
    U: Send + 'static,
    E: Send + 'static,
{
    let (mut tasks, mut input) = spawnk!(input, concurrency, f);
    let mut ret = vec![];
    while let Some(res) = tasks.join_next().await {
        let d = unpack_join_result(res).await?;
        ret.push(d);
        spawn!(tasks, input, f);
    }

    Ok(ret)
}

/// Run tasks concurrently with specified `concurrency`
pub async fn race<I, T, F, Fut>(input: I, concurrency: usize, f: F)
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (mut tasks, mut input) = spawnk!(input, concurrency, f);
    while let Some(res) = tasks.join_next().await {
        unpack_join_result(res).await;
        spawn!(tasks, input, f);
    }
}

/// Run tasks concurrently with specified `concurrency`
pub async fn try_race<I, T, E, F, Fut>(input: I, concurrency: usize, f: F) -> Result<(), E>
where
    I: IntoIterator<Item = T>,
    F: Fn(T) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: Send + 'static,
{
    let (mut tasks, mut input) = spawnk!(input, concurrency, f);
    while let Some(res) = tasks.join_next().await {
        unpack_join_result(res).await?;
        spawn!(tasks, input, f);
    }
    Ok(())
}

// copy from https://github.com/MaterializeInc/materialize/blob/dd08b8c1cd464461d97d812365a212c82049e124/src/ore/src/task.rs#L131-L152
async fn unpack_join_result<T>(res: Result<T, JoinError>) -> T {
    match res {
        Ok(val) => val,
        Err(err) => match err.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => {
                // Because `JoinHandle` and `AbortOnDropHandle` don't
                // offer `abort` method, this can only happen if the runtime is
                // shutting down, which means this `pending` won't cause a deadlock
                // because Tokio drops all outstanding futures on shutdown.
                // (In multi-threaded runtimes, not all threads drop futures simultaneously,
                // so it is possible for a future on one thread to observe the drop of a future
                // on another thread, before it itself is dropped.)
                //
                // Instead, we yield to tokio runtime. A single `yield_now` is not
                // sufficient as a `select!` or `FuturesUnordered` may
                // poll this multiple times during shutdown.
                std::future::pending().await
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::Instant;

    use super::*;

    #[tokio::test]
    async fn test_cont() {
        let data = vec![1, 2, 3, 4, 5, 6];
        let f = |t: usize| async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            t
        };

        let time = Instant::now();
        let res = cont(data.clone(), 2, f).await;
        let elapsed = time.elapsed();
        assert_eq!(res, data);
        assert_eq!(elapsed.as_secs(), 3);
    }

    #[tokio::test]
    async fn test_try_cont() {
        let data = vec![1, 2, 3, 4, 5, 6];
        let f = |t: usize| async move {
            if t == 3 {
                Err(t)
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(t)
            }
        };

        let time = Instant::now();
        let res = try_cont(data, 2, f).await;
        let elapsed = time.elapsed();
        assert_eq!(res, Err(3));
        assert_eq!(elapsed.as_secs(), 1);
    }

    #[tokio::test]
    async fn test_race() {
        let accu = Arc::new(AtomicUsize::new(0));
        let data = vec![1, 2, 3, 4, 5, 6];

        let f = |t: usize| {
            let accu = accu.clone();
            async move {
                accu.fetch_add(t, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };

        let time = Instant::now();
        race(data, 2, f).await;
        let elapsed = time.elapsed();
        assert_eq!(accu.load(Ordering::Relaxed), 21);
        assert_eq!(elapsed.as_secs(), 3);
    }

    #[tokio::test]
    async fn test_try_race() {
        let accu = Arc::new(AtomicUsize::new(0));
        let data = vec![1, 2, 3, 4, 5, 6];
        let f = |t: usize| {
            let accu = accu.clone();
            async move {
                if t == 3 {
                    Err(t)
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    accu.fetch_add(t, Ordering::Relaxed);
                    Ok(())
                }
            }
        };

        let time = Instant::now();
        let res = try_cont(data, 2, f).await;
        let elapsed = time.elapsed();
        assert_eq!(accu.load(Ordering::Relaxed), 3);
        assert_eq!(res, Err(3));
        assert_eq!(elapsed.as_secs(), 1);
    }
}
