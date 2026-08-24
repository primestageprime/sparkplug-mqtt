//! The Sparkplug B datatype codes, stated once.

/// Build [`DataType`] and both directions of its wire mapping from one table.
///
/// Each line names a variant and the number the specification gives it.
/// `code` and `from_code` are both generated from that line, so the two
/// directions cannot drift apart. Adding a datatype is one line.
macro_rules! datatypes {
    ($( $variant:ident = $code:literal, )+) => {
        /// The datatype of a metric value, as the Sparkplug B specification
        /// numbers it.
        ///
        /// A `Metric` carries this as a number in its `datatype` field. Use
        /// [`DataType::code`] to write one and [`DataType::from_code`] to
        /// read one back.
        ///
        /// This grows as the crate covers more of the specification, so it is
        /// `#[non_exhaustive]` — match with a wildcard arm.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[non_exhaustive]
        pub enum DataType {
            $( $variant, )+
        }

        impl DataType {
            /// The number this datatype carries on the wire.
            #[must_use]
            pub fn code(self) -> u32 {
                match self {
                    $( DataType::$variant => $code, )+
                }
            }

            /// The datatype a wire number names, or `None` when no datatype
            /// claims that number.
            ///
            /// Code 0 is the specification's Unknown and has no variant. The
            /// Sparkplug 3.0 array codes 22 to 31 are not covered yet, so
            /// they also return `None`.
            #[must_use]
            pub fn from_code(code: u32) -> Option<Self> {
                match code {
                    $( $code => Some(DataType::$variant), )+
                    _ => None,
                }
            }

            /// Every datatype in the table above, in code order.
            #[cfg(test)]
            const ALL: &'static [DataType] = &[ $( DataType::$variant, )+ ];
        }
    };
}

datatypes! {
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    UInt8 = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,
    Float = 9,
    Double = 10,
    Boolean = 11,
    String = 12,
    DateTime = 13,
    Text = 14,
    Uuid = 15,
    DataSet = 16,
    Bytes = 17,
    File = 18,
    Template = 19,
    PropertySet = 20,
    PropertySetList = 21,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_datatype_survives_the_round_trip() {
        for datatype in DataType::ALL {
            assert_eq!(
                DataType::from_code(datatype.code()),
                Some(*datatype),
                "{datatype:?} does not come back from its own code"
            );
        }
    }

    #[test]
    fn no_two_datatypes_share_a_code() {
        for (index, datatype) in DataType::ALL.iter().enumerate() {
            for other in &DataType::ALL[index + 1..] {
                assert_ne!(
                    datatype.code(),
                    other.code(),
                    "{datatype:?} and {other:?} claim the same code"
                );
            }
        }
    }

    #[test]
    fn the_codes_match_the_specification() {
        // Pinned against the Sparkplug B specification, section 6.4.16. A
        // renumbering here changes what every consumer reads off the wire.
        assert_eq!(DataType::Int8.code(), 1);
        assert_eq!(DataType::Int64.code(), 4);
        assert_eq!(DataType::UInt64.code(), 8);
        assert_eq!(DataType::Float.code(), 9);
        assert_eq!(DataType::Double.code(), 10);
        assert_eq!(DataType::Boolean.code(), 11);
        assert_eq!(DataType::String.code(), 12);
        assert_eq!(DataType::PropertySetList.code(), 21);
    }

    #[test]
    fn unknown_is_not_a_datatype() {
        // The old `encode_type` returned 0 for a name it did not recognise,
        // so a typo shipped Unknown and nothing reported it. Code 0 now has
        // no variant to return.
        assert_eq!(DataType::from_code(0), None);
    }

    #[test]
    fn a_code_outside_the_table_is_rejected() {
        // 22 to 31 are the Sparkplug 3.0 array types, which this crate does
        // not cover yet. They must not decode as something else.
        for code in [22, 31, 99, u32::MAX] {
            assert_eq!(
                DataType::from_code(code),
                None,
                "code {code} names no datatype this crate knows"
            );
        }
    }
}
