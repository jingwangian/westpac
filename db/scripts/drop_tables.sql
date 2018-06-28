USE db1;

IF EXISTS(SELECT * FROM sys.sysobjects WHERE name='tokenspecification')
    drop table tokenspecification;
GO

IF EXISTS(SELECT * FROM sys.sysobjects WHERE name='record')
    drop table record;
GO

IF EXISTS(SELECT * FROM sys.sysobjects WHERE name='record_token')
    drop table record_token;
GO