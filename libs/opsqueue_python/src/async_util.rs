//! Helpers to bridge async Rust and PyO3's flavour of async Python
//!
use pyo3::{Bound, IntoPyObject, PyAny, PyResult, Python};
use pyo3_async_runtimes::TaskLocals;
use std::{
    future::Future,
    pin::{pin, Pin},
    task::{Context, Poll},
};

/// Unlock the GIL across `.await` points
///
/// Essentially `py.allow_threads` but for async code.
///
/// Based on https://pyo3.rs/v0.25.1/async-await.html#release-the-gil-across-await
pub struct AsyncAllowThreads<F>(F);

pub fn async_allow_threads<F>(fut: F) -> AsyncAllowThreads<F>
where
    F: Future,
{
    AsyncAllowThreads(fut)
}

impl<F> Future for AsyncAllowThreads<F>
where
    F: Future + Unpin + Send,
    F::Output: Send,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = cx.waker();
        Python::with_gil(|py| {
            py.allow_threads(|| pin!(&mut self.0).poll(&mut Context::from_waker(waker)))
        })
    }
}

/// Version of `future_into_py` that uses the `TokioRuntimeThatIsInScope`
pub fn future_into_py<T, F>(py: Python<'_>, fut: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py>,
{
    pyo3_async_runtimes::generic::future_into_py::<TokioRuntimeThatIsInScope, F, T>(py, fut)
}

/// An alternative runtime for `pyo3_async_runtimes`
/// since `pyo3_async_runtimes::tokio` runs its _own_ Tokio runtime
/// rather than using whatever is in scope or allowing the user to pass the specific runtime.
///
/// How this runtime works, is to use whatever Tokio runtime was entered using `runtime.enter()` beforehand
struct TokioRuntimeThatIsInScope();

use once_cell::unsync::OnceCell as UnsyncOnceCell;

tokio::task_local! {
    static TASK_LOCALS: UnsyncOnceCell<TaskLocals>;
}

impl pyo3_async_runtimes::generic::Runtime for TokioRuntimeThatIsInScope {
    type JoinError = tokio::task::JoinError;
    type JoinHandle = tokio::task::JoinHandle<()>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::task::spawn(async move {
            fut.await;
        })
    }
}

impl pyo3_async_runtimes::generic::ContextExt for TokioRuntimeThatIsInScope {
    fn scope<F, R>(
        locals: TaskLocals,
        fut: F,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = R> + Send>>
    where
        F: std::future::Future<Output = R> + Send + 'static,
    {
        let cell = UnsyncOnceCell::new();
        cell.set(locals).unwrap();

        Box::pin(TASK_LOCALS.scope(cell, fut))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        TASK_LOCALS
            .try_with(|c| {
                c.get()
                    .map(|locals| Python::with_gil(|py| locals.clone_ref(py)))
            })
            .unwrap_or_default()
    }
}
