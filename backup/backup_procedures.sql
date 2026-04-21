-- =========================================
-- DATABASE BACKUP AUTOMATION PROJECT
-- Author: Kaan Alp Özdemir
-- Description: Full and differential backup with logging and alert system
-- =========================================


/* =========================================================
   1) VERİTABANI SEÇİMİ
   ========================================================= */
USE ETL_ProjeDB;
GO


/* =========================================================
   2) ESKİ NESNELERİ TEMİZLE
   ========================================================= */
IF OBJECT_ID('dbo.sp_BackupDurumSayilari', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_BackupDurumSayilari;
GO
IF OBJECT_ID('dbo.sp_AktifAlertler', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_AktifAlertler;
GO
IF OBJECT_ID('dbo.sp_SonBackupRapor', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_SonBackupRapor;
GO
IF OBJECT_ID('dbo.sp_BackupOzetRapor', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_BackupOzetRapor;
GO
IF OBJECT_ID('dbo.sp_TestHataliBackup', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_TestHataliBackup;
GO
IF OBJECT_ID('dbo.sp_DiffBackup', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_DiffBackup;
GO
IF OBJECT_ID('dbo.sp_FullBackup', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_FullBackup;
GO

IF OBJECT_ID('dbo.Backup_Alert', 'U') IS NOT NULL DROP TABLE dbo.Backup_Alert;
GO
IF OBJECT_ID('dbo.Backup_Log', 'U') IS NOT NULL DROP TABLE dbo.Backup_Log;
GO


/* =========================================================
   3) TABLOLAR
   ========================================================= */
CREATE TABLE dbo.Backup_Log
(
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    VeritabaniAdi NVARCHAR(100) NOT NULL,
    BackupTipi NVARCHAR(50) NOT NULL,
    DosyaYolu NVARCHAR(500) NOT NULL,
    Durum NVARCHAR(50) NOT NULL,
    Mesaj NVARCHAR(500) NULL,
    BaslamaTarihi DATETIME NOT NULL DEFAULT GETDATE(),
    BitisTarihi DATETIME NULL
);
GO

CREATE TABLE dbo.Backup_Alert
(
    AlertID INT IDENTITY(1,1) PRIMARY KEY,
    LogID INT NULL,
    VeritabaniAdi NVARCHAR(100) NOT NULL,
    AlertTipi NVARCHAR(50) NOT NULL,
    Aciklama NVARCHAR(500) NOT NULL,
    AlertTarihi DATETIME NOT NULL DEFAULT GETDATE(),
    Durum NVARCHAR(50) NOT NULL DEFAULT 'Aktif'
);
GO


/* =========================================================
   4) INDEXLER
   ========================================================= */
CREATE INDEX IX_Backup_Log_Durum
ON dbo.Backup_Log (Durum);
GO

CREATE INDEX IX_Backup_Log_VeritabaniAdi
ON dbo.Backup_Log (VeritabaniAdi);
GO

CREATE INDEX IX_Backup_Alert_Durum
ON dbo.Backup_Alert (Durum);
GO


/* =========================================================
   5) FULL BACKUP PROCEDURE
   SQL Server varsayılan backup klasörünü kullanır
   ========================================================= */
CREATE PROCEDURE dbo.sp_FullBackup
    @DatabaseName NVARCHAR(100) = 'ETL_ProjeDB'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BackupFolder NVARCHAR(400);
    DECLARE @DosyaYolu NVARCHAR(500);
    DECLARE @Tarih NVARCHAR(30);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @LogID INT;

    SELECT @BackupFolder = CAST(SERVERPROPERTY('InstanceDefaultBackupPath') AS NVARCHAR(400));

    IF @BackupFolder IS NULL OR LTRIM(RTRIM(@BackupFolder)) = ''
        SET @BackupFolder = 'C:\Backup';

    IF RIGHT(@BackupFolder, 1) IN ('\', '/')
        SET @BackupFolder = LEFT(@BackupFolder, LEN(@BackupFolder) - 1);

    SET @Tarih = REPLACE(CONVERT(VARCHAR(19), GETDATE(), 120), ':', '-');
    SET @DosyaYolu = @BackupFolder + '\' + @DatabaseName + '_FULL_' + @Tarih + '.bak';

    INSERT INTO dbo.Backup_Log (VeritabaniAdi, BackupTipi, DosyaYolu, Durum, Mesaj, BaslamaTarihi)
    VALUES (@DatabaseName, 'FULL', @DosyaYolu, 'Başladı', 'Full backup işlemi başlatıldı.', GETDATE());

    SET @LogID = SCOPE_IDENTITY();

    BEGIN TRY
        SET @SQL = N'BACKUP DATABASE [' + @DatabaseName + N']
                     TO DISK = N''' + @DosyaYolu + N'''
                     WITH FORMAT, INIT, STATS = 10';

        EXEC sp_executesql @SQL;

        UPDATE dbo.Backup_Log
        SET Durum = 'Başarılı',
            Mesaj = 'Full backup başarıyla tamamlandı.',
            BitisTarihi = GETDATE()
        WHERE LogID = @LogID;
    END TRY
    BEGIN CATCH
        UPDATE dbo.Backup_Log
        SET Durum = 'Hata',
            Mesaj = ERROR_MESSAGE(),
            BitisTarihi = GETDATE()
        WHERE LogID = @LogID;

        INSERT INTO dbo.Backup_Alert (LogID, VeritabaniAdi, AlertTipi, Aciklama)
        VALUES (@LogID, @DatabaseName, 'BACKUP_HATA', 'Full backup sırasında hata oluştu: ' + ERROR_MESSAGE());
    END CATCH
END;
GO


/* =========================================================
   6) DIFFERENTIAL BACKUP PROCEDURE
   ========================================================= */
CREATE PROCEDURE dbo.sp_DiffBackup
    @DatabaseName NVARCHAR(100) = 'ETL_ProjeDB'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BackupFolder NVARCHAR(400);
    DECLARE @DosyaYolu NVARCHAR(500);
    DECLARE @Tarih NVARCHAR(30);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @LogID INT;

    SELECT @BackupFolder = CAST(SERVERPROPERTY('InstanceDefaultBackupPath') AS NVARCHAR(400));

    IF @BackupFolder IS NULL OR LTRIM(RTRIM(@BackupFolder)) = ''
        SET @BackupFolder = 'C:\Backup';

    IF RIGHT(@BackupFolder, 1) IN ('\', '/')
        SET @BackupFolder = LEFT(@BackupFolder, LEN(@BackupFolder) - 1);

    SET @Tarih = REPLACE(CONVERT(VARCHAR(19), GETDATE(), 120), ':', '-');
    SET @DosyaYolu = @BackupFolder + '\' + @DatabaseName + '_DIFF_' + @Tarih + '.bak';

    INSERT INTO dbo.Backup_Log (VeritabaniAdi, BackupTipi, DosyaYolu, Durum, Mesaj, BaslamaTarihi)
    VALUES (@DatabaseName, 'DIFFERENTIAL', @DosyaYolu, 'Başladı', 'Differential backup işlemi başlatıldı.', GETDATE());

    SET @LogID = SCOPE_IDENTITY();

    BEGIN TRY
        SET @SQL = N'BACKUP DATABASE [' + @DatabaseName + N']
                     TO DISK = N''' + @DosyaYolu + N'''
                     WITH DIFFERENTIAL, INIT, STATS = 10';

        EXEC sp_executesql @SQL;

        UPDATE dbo.Backup_Log
        SET Durum = 'Başarılı',
            Mesaj = 'Differential backup başarıyla tamamlandı.',
            BitisTarihi = GETDATE()
        WHERE LogID = @LogID;
    END TRY
    BEGIN CATCH
        UPDATE dbo.Backup_Log
        SET Durum = 'Hata',
            Mesaj = ERROR_MESSAGE(),
            BitisTarihi = GETDATE()
        WHERE LogID = @LogID;

        INSERT INTO dbo.Backup_Alert (LogID, VeritabaniAdi, AlertTipi, Aciklama)
        VALUES (@LogID, @DatabaseName, 'BACKUP_HATA', 'Differential backup sırasında hata oluştu: ' + ERROR_MESSAGE());
    END CATCH
END;
GO


/* =========================================================
   7) HATA TESTİ PROCEDURE
   Bilinçli olarak yanlış yola backup alır
   ========================================================= */
CREATE PROCEDURE dbo.sp_TestHataliBackup
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DatabaseName NVARCHAR(100) = 'ETL_ProjeDB';
    DECLARE @DosyaYolu NVARCHAR(500) = 'Z:\YANLIS_KLASOR\ETL_ProjeDB_TEST.bak';
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @LogID INT;

    INSERT INTO dbo.Backup_Log (VeritabaniAdi, BackupTipi, DosyaYolu, Durum, Mesaj, BaslamaTarihi)
    VALUES (@DatabaseName, 'TEST-HATA', @DosyaYolu, 'Başladı', 'Hatalı backup testi başlatıldı.', GETDATE());

    SET @LogID = SCOPE_IDENTITY();

    BEGIN TRY
        SET @SQL = N'BACKUP DATABASE [' + @DatabaseName + N']
                     TO DISK = N''' + @DosyaYolu + N'''
                     WITH INIT';

        EXEC sp_executesql @SQL;

        UPDATE dbo.Backup_Log
        SET Durum = 'Başarılı',
            Mesaj = 'Test backup başarıyla tamamlandı.',
            BitisTarihi = GETDATE()
        WHERE LogID = @LogID;
    END TRY
    BEGIN CATCH
        UPDATE dbo.Backup_Log
        SET Durum = 'Hata',
            Mesaj = ERROR_MESSAGE(),
            BitisTarihi = GETDATE()
        WHERE LogID = @LogID;

        INSERT INTO dbo.Backup_Alert (LogID, VeritabaniAdi, AlertTipi, Aciklama)
        VALUES (@LogID, @DatabaseName, 'TEST_HATA', 'Test backup işleminde hata oluştu: ' + ERROR_MESSAGE());
    END CATCH
END;
GO


/* =========================================================
   8) RAPORLAMA PROCEDURE'LARI
   ========================================================= */
CREATE PROCEDURE dbo.sp_BackupOzetRapor
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        VeritabaniAdi,
        BackupTipi,
        Durum,
        COUNT(*) AS KayitSayisi
    FROM dbo.Backup_Log
    GROUP BY VeritabaniAdi, BackupTipi, Durum
    ORDER BY VeritabaniAdi, BackupTipi, Durum;
END;
GO

CREATE PROCEDURE dbo.sp_SonBackupRapor
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        VeritabaniAdi,
        BackupTipi,
        MAX(BitisTarihi) AS SonBackupTarihi
    FROM dbo.Backup_Log
    WHERE Durum = 'Başarılı'
    GROUP BY VeritabaniAdi, BackupTipi
    ORDER BY VeritabaniAdi, BackupTipi;
END;
GO

CREATE PROCEDURE dbo.sp_AktifAlertler
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        AlertID,
        LogID,
        VeritabaniAdi,
        AlertTipi,
        Aciklama,
        AlertTarihi,
        Durum
    FROM dbo.Backup_Alert
    WHERE Durum = 'Aktif'
    ORDER BY AlertID DESC;
END;
GO

CREATE PROCEDURE dbo.sp_BackupDurumSayilari
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        COUNT(*) AS ToplamBackupKaydi,
        SUM(CASE WHEN Durum = 'Başarılı' THEN 1 ELSE 0 END) AS BasariliBackupSayisi,
        SUM(CASE WHEN Durum = 'Hata' THEN 1 ELSE 0 END) AS HataliBackupSayisi
    FROM dbo.Backup_Log;
END;
GO


/* =========================================================
   9) SQL SERVER AGENT İÇİN ÖRNEK JOB SCRIPTİ
   İstersen sadece rapora koyarsın, çalıştırmak zorunlu değil
   ========================================================= */
/*
USE msdb;
GO

EXEC dbo.sp_add_job
    @job_name = N'ETL_ProjeDB_FullBackup_Job',
    @enabled = 1,
    @description = N'ETL_ProjeDB için otomatik full backup job''ı';
GO

EXEC dbo.sp_add_jobstep
    @job_name = N'ETL_ProjeDB_FullBackup_Job',
    @step_name = N'Full Backup Step',
    @subsystem = N'TSQL',
    @command = N'EXEC ETL_ProjeDB.dbo.sp_FullBackup;',
    @database_name = N'ETL_ProjeDB';
GO

EXEC dbo.sp_add_schedule
    @schedule_name = N'GunlukYedek',
    @freq_type = 4,
    @freq_interval = 1,
    @active_start_time = 230000;
GO

EXEC dbo.sp_attach_schedule
    @job_name = N'ETL_ProjeDB_FullBackup_Job',
    @schedule_name = N'GunlukYedek';
GO

EXEC dbo.sp_add_jobserver
    @job_name = N'ETL_ProjeDB_FullBackup_Job';
GO
*/


/* =========================================================
   10) BACKUP YOLUNU GÖR
   ========================================================= */
SELECT CAST(SERVERPROPERTY('InstanceDefaultBackupPath') AS NVARCHAR(400)) AS VarsayilanBackupYolu;
GO


/* =========================================================
   11) ÇALIŞTIRMA
   Önce full backup, sonra diff backup, sonra hata testi
   ========================================================= */
EXEC dbo.sp_FullBackup;
GO

EXEC dbo.sp_DiffBackup;
GO

EXEC dbo.sp_TestHataliBackup;
GO


/* =========================================================
   12) RAPORLAR
   ========================================================= */
SELECT *
FROM dbo.Backup_Log
ORDER BY LogID DESC;
GO

SELECT *
FROM dbo.Backup_Alert
ORDER BY AlertID DESC;
GO

EXEC dbo.sp_BackupOzetRapor;
GO

EXEC dbo.sp_SonBackupRapor;
GO

EXEC dbo.sp_AktifAlertler;
GO

EXEC dbo.sp_BackupDurumSayilari;
GO


/* =========================================================
   13) EK RAPOR - msdb ÜZERİNDEN KONTROL
   Başarılı backup varsa burada görünür
   ========================================================= */
SELECT
    bs.database_name AS VeritabaniAdi,
    CASE bs.type
        WHEN 'D' THEN 'FULL'
        WHEN 'I' THEN 'DIFFERENTIAL'
        WHEN 'L' THEN 'LOG'
        ELSE bs.type
    END AS BackupTipi,
    MAX(bs.backup_finish_date) AS SonBackupTarihi
FROM msdb.dbo.backupset bs
WHERE bs.database_name = 'ETL_ProjeDB'
GROUP BY bs.database_name, bs.type
ORDER BY BackupTipi;
GO
