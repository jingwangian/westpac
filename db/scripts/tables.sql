USE db1;

IF NOT EXISTS(SELECT * FROM sys.sysobjects WHERE name='tokenspecification')
    create table tokenspecification (
        id int primary key,
        account_id varchar(256),
        code varchar(256),
        name varchar(256));
GO

IF NOT EXISTS(SELECT * FROM sys.sysobjects WHERE name='record')
    create table record (
    id int,
    natural_key varchar(256) primary key,
    family_names varchar(256),
    first_name varchar(256),
    gender char(1),
    modified_date datetime);
GO

IF NOT EXISTS(SELECT * FROM sys.sysobjects WHERE name='record_token')
    create table record_token(
        id int,
        natural_key varchar(256),
        code varchar(256),
        token varchar(64),
        family_names varchar(256),
        primary key(natural_key,code));

GO