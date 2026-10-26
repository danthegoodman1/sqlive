//! Query parameters, as values SqLive can both bind and read.

use rusqlite::ToSql;
use rusqlite::types::{ToSqlOutput, Value};

/// A query's parameters, read out as values so a read set can record the
/// keys a query asked for.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ParamValues {
    /// Values for `?1`, `?2`, ...
    pub positional: Vec<Value>,
    /// Values for `:name`, `@name` and `$name`, with their prefix.
    pub named: Vec<(String, Value)>,
}

/// Parameters for [`crate::LiveDb::query`]: arrays, vectors and tuples of
/// [`ToSql`] values, what rusqlite's `params!` and `named_params!` produce,
/// or a [`ParamValues`].
pub trait QueryParams {
    fn into_values(self) -> rusqlite::Result<ParamValues>;
}

pub(crate) fn value_of(param: &dyn ToSql) -> rusqlite::Result<Value> {
    Ok(match param.to_sql()? {
        ToSqlOutput::Borrowed(value) => crate::capture::owned(value),
        ToSqlOutput::Owned(value) => value,
        other => {
            return Err(rusqlite::Error::ToSqlConversionFailure(
                format!("SqLive cannot read this parameter back: {other:?}").into(),
            ));
        }
    })
}

impl QueryParams for ParamValues {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        Ok(self)
    }
}

impl QueryParams for () {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        Ok(ParamValues::default())
    }
}

impl<T: ToSql, const N: usize> QueryParams for [T; N] {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        positional(self.iter().map(|p| p as &dyn ToSql))
    }
}

impl QueryParams for &[&dyn ToSql] {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        positional(self.iter().copied())
    }
}

impl<T: ToSql> QueryParams for Vec<T> {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        positional(self.iter().map(|p| p as &dyn ToSql))
    }
}

impl<const N: usize> QueryParams for &[&dyn ToSql; N] {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        positional(self.iter().copied())
    }
}

impl<const N: usize> QueryParams for &[(&str, &dyn ToSql); N] {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        self.as_slice().into_values()
    }
}

impl QueryParams for &[(&str, &dyn ToSql)] {
    fn into_values(self) -> rusqlite::Result<ParamValues> {
        let named = self
            .iter()
            .map(|(name, value)| Ok(((*name).to_owned(), value_of(*value)?)))
            .collect::<rusqlite::Result<_>>()?;
        Ok(ParamValues {
            positional: Vec::new(),
            named,
        })
    }
}

fn positional<'a>(params: impl Iterator<Item = &'a dyn ToSql>) -> rusqlite::Result<ParamValues> {
    Ok(ParamValues {
        positional: params.map(value_of).collect::<rusqlite::Result<_>>()?,
        named: Vec::new(),
    })
}

macro_rules! tuple_params {
    ($($name:ident),+) => {
        impl<$($name: ToSql),+> QueryParams for ($($name,)+) {
            #[allow(non_snake_case)]
            fn into_values(self) -> rusqlite::Result<ParamValues> {
                let ($($name,)+) = self;
                positional([$(&$name as &dyn ToSql),+].into_iter())
            }
        }
    };
}

tuple_params!(A);
tuple_params!(A, B);
tuple_params!(A, B, C);
tuple_params!(A, B, C, D);
tuple_params!(A, B, C, D, E);
tuple_params!(A, B, C, D, E, F);
tuple_params!(A, B, C, D, E, F, G);
tuple_params!(A, B, C, D, E, F, G, H);
