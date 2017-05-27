use std::rc::Rc;
use std::cell::RefCell;
use std::borrow::Cow;

use futures::Future;

use errors::ErrorKind;
use protocol::{KafkaCode, Schema};
use client::{BrokerRef, Client, ConsumerGroupProtocol, Generation, KafkaClient, StaticBoxFuture};
use consumer::{CONSUMER_PROTOCOL, PartitionAssignor, Subscriptions};

/// Manages the coordination process with the consumer coordinator.
pub trait Coordinator {
    /// Join the consumer group.
    fn join_group(&mut self) -> JoinGroup;

    /// Leave the current consumer group.
    fn leave_group(&mut self) -> LeaveGroup;
}

pub type JoinGroup = StaticBoxFuture;

pub type LeaveGroup = StaticBoxFuture;

/// Manages the coordination process with the consumer coordinator.
pub struct ConsumerCoordinator<'a> {
    client: KafkaClient<'a>,
    group_id: String,
    subscriptions: Subscriptions,
    session_timeout: i32,
    rebalance_timeout: i32,
    assignors: Vec<Box<PartitionAssignor>>,
    state: Rc<RefCell<State>>,
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

impl<'a> ConsumerCoordinator<'a> {
    pub fn new(client: KafkaClient<'a>,
               group_id: String,
               subscriptions: Subscriptions,
               session_timeout: i32,
               rebalance_timeout: i32,
               assignors: Vec<Box<PartitionAssignor>>)
               -> Self {
        ConsumerCoordinator {
            client: client,
            group_id: group_id,
            subscriptions: subscriptions,
            session_timeout: session_timeout,
            rebalance_timeout: rebalance_timeout,
            assignors: assignors,
            state: Rc::new(RefCell::new(State::Unjoined)),
        }
    }

    fn group_protocols(&self) -> Vec<ConsumerGroupProtocol<'a>> {
        let topics = self.subscriptions.topics();

        let group_protocols = self.assignors
            .iter()
            .flat_map(move |assignor| {
                let subscription = assignor.subscription(topics
                                                             .iter()
                                                             .map(|topic_name| {
                                                                      String::from(*topic_name)
                                                                          .into()
                                                                  })
                                                             .collect());
                Schema::serialize(&subscription)
                    .map_err(|err| warn!("fail to serialize subscription, {}", err))
                    .ok()
                    .map(|metadata| {
                             ConsumerGroupProtocol {
                                 protocol_name: assignor.name().into(),
                                 protocol_metadata: metadata.into(),
                             }
                         })
            })
            .collect();

        group_protocols
    }
}

impl<'a> Coordinator for ConsumerCoordinator<'a>
    where Self: 'static
{
    fn join_group(&mut self) -> JoinGroup {
        (*self.state.borrow_mut()) = State::Rebalancing;

        let client = self.client.clone();
        let group_id: Cow<'a, str> = self.group_id.clone().into();
        let session_timeout = self.session_timeout;
        let rebalance_timeout = self.rebalance_timeout;
        let generation = Generation::default();
        let member_id = generation.member_id.into();
        let group_protocols = self.group_protocols();
        let state = self.state.clone();

        let future = self.client
            .group_coordinator(group_id.clone())
            .and_then(move |coordinator| {
                client
                    .join_group(coordinator.as_ref(),
                                group_id,
                                session_timeout,
                                rebalance_timeout,
                                member_id,
                                CONSUMER_PROTOCOL.into(),
                                group_protocols)
                    .and_then(move |consumer_group| {
                        let generation = consumer_group.generation();

                        let group_assignment = if consumer_group.is_leader() {
                            vec![]
                        } else {
                            vec![]
                        };

                        client
                            .sync_group(coordinator.as_ref(),
                                        consumer_group.group_id.clone().into(),
                                        generation.generation_id.into(),
                                        generation.member_id.clone().into(),
                                        group_assignment)
                            .then(move |result| {
                                *state.borrow_mut() = match result {
                                    Ok(_) => {
                                        State::Stable {
                                            coordinator: coordinator.as_ref(),
                                            generation: generation,
                                        }
                                    }
                                    Err(_) => State::Unjoined,
                                };

                                result.map(|_| ())
                            })
                    })
            });

        JoinGroup::new(future)
    }

    fn leave_group(&mut self) -> LeaveGroup {
        if let State::Stable {
                   coordinator,
                   ref generation,
               } = *self.state.borrow() {
            debug!("sending LeaveGroup request to coordinator {:?} for group `{}`",
                   coordinator,
                   self.group_id);

            (*self.state.borrow_mut()) = State::Rebalancing;

            let group_id = self.group_id.clone().into();
            let state = self.state.clone();

            LeaveGroup::new(self.client
                                .leave_group(coordinator,
                                             group_id,
                                             generation.member_id.clone().into())
                                .map(move |group_id| {
                                         debug!("leaved group {}", group_id);

                                         *state.borrow_mut() = State::Unjoined;
                                     }))
        } else {
            LeaveGroup::err(ErrorKind::KafkaError(KafkaCode::GroupLoadInProgress).into())
        }
    }
}
