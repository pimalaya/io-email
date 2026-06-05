//! # Search emails filter query
//!
//! Exposes [`SearchEmailsFilterQuery`], the recursive AST produced by
//! [`super::parser::query`].

use alloc::{boxed::Box, string::String};

use chrono::NaiveDate;

use crate::flag::types::Flag;

/// The search emails filter query.
///
/// Composed of 3 operators (and, or, not) and 7 conditions (date,
/// after date, from, to, subject, body, flag). All date-related
/// conditions are anchored to the `Date:` header (sent-at), never to
/// the server-side received-at timestamp.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SearchEmailsFilterQuery {
    /// Filter emails that match both given conditions.
    And(Box<SearchEmailsFilterQuery>, Box<SearchEmailsFilterQuery>),

    /// Filter emails that match one of the two given conditions.
    Or(Box<SearchEmailsFilterQuery>, Box<SearchEmailsFilterQuery>),

    /// Filter emails that do not match the given condition.
    Not(Box<SearchEmailsFilterQuery>),

    /// Filter emails where the `Date:` header of the message matches
    /// the given date.
    ///
    /// Only the year, the month and the day are taken into
    /// consideration.
    Date(NaiveDate),

    /// Filter emails where the `Date:` header of the message is
    /// strictly greater than the given date.
    ///
    /// For example, for `2024-01-01` it matches messages with a date
    /// starting from `2024-01-02` and above. Only the year, the month
    /// and the day are taken into consideration.
    AfterDate(NaiveDate),

    /// Filter emails where the `From:` header of the message contains
    /// the given pattern.
    From(String),

    /// Filter emails where the `To:` header of the message contains
    /// the given pattern.
    To(String),

    /// Filter emails where the `Subject:` header of the message
    /// contains the given pattern.
    Subject(String),

    /// Filter emails where one of the text bodies of the message
    /// contains the given pattern.
    Body(String),

    /// Filter emails where the given flag is included in the email
    /// envelope flags.
    Flag(Flag),
}
