use futures::future;

use protocol::GenerationId;
use client::{BrokerRef, Client, KafkaClient, StaticBoxFuture};

/// Manages the coordination process with the consumer coordinator.
pub trait Coordinator {
    /// Leave the current group.
    fn leave_group(&self) -> LeaveGroup;
}

pub type LeaveGroup = StaticBoxFuture<String>;

/// Manages the coordination process with the consumer coordinator.
pub struct ConsumerCoordinator<'a> {
    client: KafkaClient<'a>,
    group_id: String,
    state: State,
}

enum State {
    /// the client is not part of a group
    Unjoined,
    /// the client has begun rebalancing
    Rebalancing,
    /// the client has joined and is sending heartbeats
    Stable {
        coordinator: BrokerRef,
        generation: Generation,
    },
}

struct Generation {
    generation_id: GenerationId,
    member_id: String,
    protocol: String,
}

impl<'a> ConsumerCoordinator<'a> {
    pub fn new(client: KafkaClient<'a>, group_id: String) -> Self {
        ConsumerCoordinator {
            client: client,
            group_id: group_id,
            state: State::Unjoined,
        }
    }
}

impl<'a> Coordinator for ConsumerCoordinator<'a>
    where Self: 'static
{
    fn leave_group(&self) -> LeaveGroup {
        if let State::Stable {
                   coordinator,
                   ref generation,
               } = self.state {
            debug!("sending LeaveGroup request to coordinator {:?} for group `{}`",
                   coordinator,
                   self.group_id);

            self.client
                .leave_group(coordinator,
                             self.group_id.clone().into(),
                             generation.member_id.clone().into())
        } else {
            LeaveGroup::new(future::ok(self.group_id.clone()))
        }
    }
}
