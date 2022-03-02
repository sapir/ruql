# ruql - Rule Query Language

A WIP experimental database query language, inspired by
[Logica](https://logica.dev/).

```
ruql> data cities(name, country) =
 ...>     ("Jerusalem", "Israel"),
 ...>     ("Paris", "France"),
 ...>     ("London", "England")
 ...> ;
ruql> israeli_cities(name) =
 ...>     cities(name, country),
 ...>     country = "Israel"
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
