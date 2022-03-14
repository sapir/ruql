# ruql - Rule Query Language

A WIP experimental database query language, inspired by
[Logica](https://logica.dev/).

The main binary currently runs queries against an sqlite database specified as a
commandline argument. If no argument is specified, queries are instead run
against an empty in-memory database; query data can still be specified in ruql
syntax, though.

```
ruql> data cities(name, country) =
 ...>     ("Jerusalem", "Israel"),
 ...>     ("Paris", "France"),
 ...>     ("London", "England"),
 ...> ;
ruql> israeli_cities(name) =
 ...>     cities(name, country),
 ...>     country = "Israel",
 ...> ;
ruql> israeli_cities(name);
Jerusalem
ruql> cities(name);
Jerusalem
London
Paris
ruql> cities(country);
Israel
England
France
ruql>
```
