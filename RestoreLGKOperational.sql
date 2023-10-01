use master;

declare @folderFull nvarchar(255) = 'E:\D6-backup\D6-DB\LGKOperational\Full'
declare @folderDiff nvarchar(255) = 'E:\D6-backup\D6-DB\LGKOperational\Minutly'
declare @sql nvarchar(max);
declare @fileNameFull nvarchar(255);
declare @fileNameDiff nvarchar(255);
declare @lastRestoredDateTime datetime;
declare @FullBackupDateTime datetime;
declare @DiffBackupDateTime datetime;
declare @CheckpointLSN numeric(25,0) -- Это идентификатор полной копии
declare @DatabaseBackupLSN numeric(25,0) -- Это идентификатор дифф. копии, должен совпадать с полной

declare @filesFull table (
 file__Name nvarchar(255),
 depth tinyint,
 isFile bit
);

declare @filesDiff table (
 file__Name nvarchar(255),
 depth tinyint,
 isFile bit
);

set @sql = 'exec master.sys.xp_dirtree ''' + @folderFull + ''' , 0, 1;';
insert into @filesFull exec sp_executesql @sql;
select TOP 1 @fileNameFull = file__Name from @filesFull order by file__Name desc
--select @fileNameFull

set @sql = 'exec master.sys.xp_dirtree ''' + @folderDiff + ''' , 0, 1;';
insert into @filesDiff exec sp_executesql @sql;
select TOP 1 @fileNameDiff = file__Name from @filesDiff order by file__Name desc;
--select @fileNameDiff;


WITH LastRestores AS
(
SELECT
    DatabaseName = [d].[name] ,
    [d].[create_date] ,
    [d].[compatibility_level] ,
    [d].[collation_name] ,
    r.*,
    RowNum = ROW_NUMBER() OVER (PARTITION BY d.Name ORDER BY r.[restore_date] DESC)
FROM master.sys.databases d
LEFT OUTER JOIN msdb.dbo.[restorehistory] r ON r.[destination_database_name] = d.Name
)
SELECT @lastRestoredDateTime = restore_date
FROM [LastRestores]
Where
[DatabaseName] = 'LGKOperational' and [RowNum] = 1;

--select @lastRestoredDateTime;


declare @headers table 
( 
    BackupName varchar(256),
    BackupDescription varchar(256),
    BackupType varchar(256),        
    ExpirationDate varchar(256),
    Compressed varchar(256),
    Position varchar(256),
    DeviceType varchar(256),        
    UserName varchar(256),
    ServerName varchar(256),
    DatabaseName varchar(256),
    DatabaseVersion varchar(256),        
    DatabaseCreationDate varchar(256),
    BackupSize varchar(256),
    FirstLSN varchar(256),
    LastLSN varchar(256),        
    CheckpointLSN varchar(256),
    DatabaseBackupLSN varchar(256),
    BackupStartDate datetime,
    BackupFinishDate varchar(256),        
    SortOrder varchar(256),
    CodePage varchar(256),
    UnicodeLocaleId varchar(256),
    UnicodeComparisonStyle varchar(256),        
    CompatibilityLevel varchar(256),
    SoftwareVendorId varchar(256),
    SoftwareVersionMajor varchar(256),        
    SoftwareVersionMinor varchar(256),
    SoftwareVersionBuild varchar(256),
    MachineName varchar(256),
    Flags varchar(256),        
    BindingID varchar(256),
    RecoveryForkID varchar(256),
    Collation varchar(256),
    FamilyGUID varchar(256),        
    HasBulkLoggedData varchar(256),
    IsSnapshot varchar(256),
    IsReadOnly varchar(256),
    IsSingleUser varchar(256),        
    HasBackupChecksums varchar(256),
    IsDamaged varchar(256),
    BeginsLogChain varchar(256),
    HasIncompleteMetaData varchar(256),        
    IsForceOffline varchar(256),
    IsCopyOnly varchar(256),
    FirstRecoveryForkID varchar(256),
    ForkPointLSN varchar(256),        
    RecoveryModel varchar(256),
    DifferentialBaseLSN varchar(256),
    DifferentialBaseGUID varchar(256),        
    BackupTypeDescription varchar(256),
    BackupSetGUID varchar(256),
    CompressedBackupSize varchar(256),        
    Containment varchar(256),
	KeyAlgorithm nvarchar(32),
	EncryptorThumbprint varbinary(20),
	EncryptorType nvarchar(32),
    --
    -- This field added to retain order by
    --
    Seq int NOT NULL identity(1,1)
); 

insert into @headers exec('restore headeronly from disk = '''+ @folderFull + '\' + @fileNameFull + '''');
insert into @headers exec('restore headeronly from disk = '''+ @folderDiff + '\' + @fileNameDiff + '''');
--select * from @headers;
select @CheckpointLSN = CheckpointLSN, @FullBackupDateTime = BackupStartDate from @headers where Seq=1;
select @DatabaseBackupLSN = DatabaseBackupLSN, @DiffBackupDateTime = BackupStartDate from @headers where Seq=2;
select @CheckpointLSN as CheckpointLSN, @DatabaseBackupLSN as DatabaseBackupLSN,  @FullBackupDateTime as FullBackupDateTime, 
       @DiffBackupDateTime as DiffBackupDateTime, @lastRestoredDateTime as lastRestoredDateTime;


IF @lastRestoredDateTime < @FullBackupDateTime
  BEGIN
	-- Загружаем полный бэкап
	SET @sql = 
	N'RESTORE DATABASE [LGKOperational]
	FROM DISK = N''' + @folderFull + '\' + @fileNameFull + ''' 
	WITH  
	FILE = 1,
    NORECOVERY,
	REPLACE,
	STATS = 5';

    -- Выводим и выполняем полученную инструкцию
	PRINT @sql;
	EXEC sp_executesql @sql;
  END;

  IF (@CheckpointLSN = @DatabaseBackupLSN AND @DiffBackupDateTime > @lastRestoredDateTime)
    BEGIN
	    -- Загружаем разностный бэкап 
		set @sql = 
			N'RESTORE DATABASE LGKOperational 
			FROM DISK = ''' + @folderDiff + '\' + @fileNameDiff + '''
			WITH
			FILE = 1,
			NORECOVERY,
			STATS = 5';
			
			-- Выводим и выполняем полученную инструкцию
			PRINT @sql;	
			EXEC sp_executesql @sql

	END;