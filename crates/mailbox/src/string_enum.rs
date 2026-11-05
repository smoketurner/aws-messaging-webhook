//! Enums whose variants each have one string form, written once.
//!
//! `string_enum!` is for a closed set this service owns: the string is the
//! serde name, `as_str`, and the entry in `ALL`, so they cannot drift apart.
//! `open_string_enum!` is for values AWS supplies: the known ones get
//! variants, and anything else parses into `Other` and serializes back
//! verbatim, so a value AWS adds later neither fails the parse nor changes
//! what is stored or forwarded.

#[macro_export]
macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $wire:literal, )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        $vis enum $name {
            $( $(#[$variant_meta])* #[serde(rename = $wire)] $variant, )+
        }

        impl $name {
            /// Every variant, in declaration order.
            pub const ALL: [Self; [$($wire),+].len()] = [$(Self::$variant),+];

            /// The stored and published form, identical to the serde name.
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire,)+
                }
            }
        }
    };
}

#[macro_export]
macro_rules! open_string_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $wire:literal, )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        #[serde(from = "String", into = "String")]
        $vis enum $name {
            $( $(#[$variant_meta])* $variant, )+
            /// A value this service has no variant for, kept verbatim.
            Other(String),
        }

        impl $name {
            /// The value as AWS sent it.
            #[must_use]
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $wire,)+
                    Self::Other(value) => value,
                }
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                match value.as_str() {
                    $($wire => Self::$variant,)+
                    _ => Self::Other(value),
                }
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                match value {
                    $($name::$variant => $wire.to_owned(),)+
                    $name::Other(value) => value,
                }
            }
        }
    };
}
