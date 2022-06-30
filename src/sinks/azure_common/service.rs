use std::{
    result::Result as StdResult,
    sync::Arc,
    task::{Context, Poll},
};

use azure_core::error::HttpError;
use azure_storage_blobs::{prelude::*, blob::operations::PutBlockBlobResponse};
use futures::{future::BoxFuture, TryFutureExt};
use tower::Service;
use tracing::Instrument;

use crate::{
    internal_events::azure_blob::{AzureBlobHttpError, AzureBlobResponseError},
    sinks::azure_common::config::{AzureBlobRequest, AzureBlobResponse},
};
use vector_common::internal_event::BytesSent;

#[derive(Clone)]
pub(crate) struct AzureBlobService {
    pub(self) client: Arc<ContainerClient>,
}

impl AzureBlobService {
    pub const fn new(client: Arc<ContainerClient>) -> AzureBlobService {
        AzureBlobService { client }
    }
}

impl Service<AzureBlobRequest> for AzureBlobService {
    type Response = AzureBlobResponse;
    type Error = Box<dyn std::error::Error + std::marker::Send + std::marker::Sync>;
    type Future = BoxFuture<'static, StdResult<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<StdResult<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: AzureBlobRequest) -> Self::Future {
        let client =
            Arc::clone(&self.client).blob_client(request.metadata.partition_key.as_str());

        Box::pin(async move {
            let byte_size = request.blob_data.len();
            let blob = client
                .put_block_blob(request.blob_data)
                .content_type(request.content_type);
            let blob = match request.content_encoding {
                Some(encoding) => blob.content_encoding(encoding),
                None => blob,
            };

            let result: Result<PutBlockBlobResponse, Self::Error>  = blob
                .into_future()
                .inspect_err(|reason| {
                    match reason.downcast_ref::<HttpError>() {
                        Some(err) => {
                            emit!(AzureBlobResponseError::from(err.status()))
                        }
                        _ => emit!(AzureBlobHttpError {
                            error: reason.to_string()
                        }),
                    };
                })
                .inspect_ok(|_| {
                    emit!(BytesSent {
                        byte_size,
                        protocol: "https",
                    });
                })
                .instrument(info_span!("request").or_current())
                .await.map_err(|err| err.into());

            result.map(|inner| AzureBlobResponse {
                inner,
                count: request.metadata.count,
                events_byte_size: request.metadata.byte_size,
            })
        })
    }
}
