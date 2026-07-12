// SPDX-License-Identifier: Apache-2.0
//! Service↔broker topic control messages (spec 03), templates 7–15.
//!
//! The wire structs, encoders, and decoders now live in `ringloom_common`
//! (`src/common/message/topic_control_messages.zig`) so the service runtime
//! and every language binding share one source of truth. This module is a
//! thin re-export shim kept for existing broker-side imports.

const common = @import("ringloom_common");

pub const TEMPLATE_REGISTER_TOPIC_PUBLICATION = common.message.topic_control_messages.TEMPLATE_REGISTER_TOPIC_PUBLICATION;
pub const TEMPLATE_TOPIC_PUBLICATION_RESPONSE = common.message.topic_control_messages.TEMPLATE_TOPIC_PUBLICATION_RESPONSE;
pub const TEMPLATE_SUBSCRIBE_TOPIC = common.message.topic_control_messages.TEMPLATE_SUBSCRIBE_TOPIC;
pub const TEMPLATE_TOPIC_SUBSCRIPTION_RESPONSE = common.message.topic_control_messages.TEMPLATE_TOPIC_SUBSCRIPTION_RESPONSE;
pub const TEMPLATE_UNREGISTER_TOPIC_PUBLICATION = common.message.topic_control_messages.TEMPLATE_UNREGISTER_TOPIC_PUBLICATION;
pub const TEMPLATE_UNSUBSCRIBE_TOPIC = common.message.topic_control_messages.TEMPLATE_UNSUBSCRIBE_TOPIC;
pub const TEMPLATE_TOPIC_LEADER_CHANGED = common.message.topic_control_messages.TEMPLATE_TOPIC_LEADER_CHANGED;
pub const TEMPLATE_TOPIC_ENDPOINT = common.message.topic_control_messages.TEMPLATE_TOPIC_ENDPOINT;
pub const TEMPLATE_TOPIC_ACK_FEEDBACK = common.message.topic_control_messages.TEMPLATE_TOPIC_ACK_FEEDBACK;

pub const PublicationStatus = common.message.topic_control_messages.PublicationStatus;
pub const SubscriptionStatus = common.message.topic_control_messages.SubscriptionStatus;
pub const StartPosition = common.message.topic_control_messages.StartPosition;

pub const RegisterTopicPublicationMsg = common.message.topic_control_messages.RegisterTopicPublicationMsg;
pub const TopicPublicationResponseMsg = common.message.topic_control_messages.TopicPublicationResponseMsg;
pub const SubscribeTopicMsg = common.message.topic_control_messages.SubscribeTopicMsg;
pub const TopicSubscriptionResponseMsg = common.message.topic_control_messages.TopicSubscriptionResponseMsg;
pub const UnregisterTopicPublicationMsg = common.message.topic_control_messages.UnregisterTopicPublicationMsg;
pub const UnsubscribeTopicMsg = common.message.topic_control_messages.UnsubscribeTopicMsg;
pub const TopicLeaderChangedMsg = common.message.topic_control_messages.TopicLeaderChangedMsg;
pub const TopicEndpointMsg = common.message.topic_control_messages.TopicEndpointMsg;
pub const TopicAckFeedbackMsg = common.message.topic_control_messages.TopicAckFeedbackMsg;

pub const encodeRegisterTopicPublication = common.message.topic_control_messages.encodeRegisterTopicPublication;
pub const encodeSubscribeTopic = common.message.topic_control_messages.encodeSubscribeTopic;
pub const encodePublicationResponse = common.message.topic_control_messages.encodePublicationResponse;
pub const encodeSubscriptionResponse = common.message.topic_control_messages.encodeSubscriptionResponse;
pub const encodeAckFeedback = common.message.topic_control_messages.encodeAckFeedback;
pub const encodeTopicLeaderChanged = common.message.topic_control_messages.encodeTopicLeaderChanged;
pub const encodeUnregisterTopicPublication = common.message.topic_control_messages.encodeUnregisterTopicPublication;
pub const encodeUnsubscribeTopic = common.message.topic_control_messages.encodeUnsubscribeTopic;

pub const decode = common.message.topic_control_messages.decode;
pub const registerTopicName = common.message.topic_control_messages.registerTopicName;
pub const subscribeTopicName = common.message.topic_control_messages.subscribeTopicName;
pub const subscriptionResponseQueueDir = common.message.topic_control_messages.subscriptionResponseQueueDir;
