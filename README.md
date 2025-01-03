# MySQLite

A clone of  [sqlite](https://www.sqlite.org/arch.html) in C.

## Building
To build the project simply run the [Makefile](./Makefile) at the root of the project with one of the following configurations
- `make all`
- `make debug`
- `make release`

## Running tests

[Ruby](https://www.ruby-lang.org/en/downloads/) is required to run the tests.
    
- Download the latest Ruby version for your OS.
- Run `gem install rspec` to install the required libraries.
- Build the project using the debug configuration.
- At the root of the project run the command `rspec ./tests/spec/database_spec.rb`

## Sqlite Architecture

![Showcase](sqlite%20arch.gif)