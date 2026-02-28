//! Message trait and fan-in macro for combining multiple message types.

/// Marker trait for messages that can be sent to an actor's mailbox.
///
/// All message types must be `Send + 'static` so they can cross
/// async task boundaries.
pub trait Message: Send + 'static {}

// Blanket impl: any Send + 'static type is a valid message.
impl<T: Send + 'static> Message for T {}

/// Generate a fan-in enum that combines multiple message types into one.
///
/// This is useful when an actor needs to handle messages from multiple
/// upstream producers, each sending a different type.
///
/// # Example
///
/// ```rust
/// use varpulis_actors::fan_in_message_type;
///
/// #[derive(Debug)]
/// struct Tick;
/// #[derive(Debug)]
/// struct Flush;
///
/// fan_in_message_type!(TimerMessage, Tick, Flush);
///
/// // Now you can do:
/// let msg: TimerMessage = Tick.into();
/// let msg2: TimerMessage = Flush.into();
/// ```
#[macro_export]
macro_rules! fan_in_message_type {
    ($name:ident, $($variant:ident),+ $(,)?) => {
        #[derive(Debug)]
        pub enum $name {
            $($variant($variant),)+
        }

        $(
            impl From<$variant> for $name {
                fn from(msg: $variant) -> Self {
                    $name::$variant(msg)
                }
            }
        )+
    };
}
