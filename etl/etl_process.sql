-- =========================================
-- ETL DATA CLEANING PROJECT
-- Author: Kaan Alp Özdemir
-- Description: Raw data cleaning and transformation process
-- =========================================


/* =========================================================
   1) VERITABANI OLUŞTURMA
   ========================================================= */
IF DB_ID('ETL_ProjeDB') IS NOT NULL
BEGIN
    ALTER DATABASE ETL_ProjeDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE ETL_ProjeDB;
END;
GO

CREATE DATABASE ETL_ProjeDB;
GO

USE ETL_ProjeDB;
GO


/* =========================================================
   2) TABLOLAR
   ========================================================= */

/* Ham veri tablosu */
CREATE TABLE Musteri_Raw
(
    RawID INT IDENTITY(1,1) PRIMARY KEY,
    Ad NVARCHAR(50),
    Soyad NVARCHAR(50),
    Email NVARCHAR(100),
    Telefon NVARCHAR(30),
    DogumTarihi NVARCHAR(30),
    Sehir NVARCHAR(50),
    KayitTarihi DATETIME DEFAULT GETDATE()
);
GO

/* Staging tablosu */
CREATE TABLE Musteri_Staging
(
    StagingID INT IDENTITY(1,1) PRIMARY KEY,
    RawID INT NOT NULL,
    Ad NVARCHAR(50),
    Soyad NVARCHAR(50),
    Email NVARCHAR(100),
    Telefon NVARCHAR(20),
    DogumTarihi DATE NULL,
    Sehir NVARCHAR(50),
    HataNedeni NVARCHAR(300) NULL,
    YuklemeTarihi DATETIME DEFAULT GETDATE()
);
GO

/* Temiz veri tablosu */
CREATE TABLE Musteri_Clean
(
    MusteriID INT IDENTITY(1,1) PRIMARY KEY,
    Ad NVARCHAR(50) NOT NULL,
    Soyad NVARCHAR(50) NOT NULL,
    Email NVARCHAR(100) NOT NULL,
    Telefon NVARCHAR(20) NOT NULL,
    DogumTarihi DATE NULL,
    Sehir NVARCHAR(50) NULL,
    KaynakRawID INT NOT NULL,
    EklenmeTarihi DATETIME DEFAULT GETDATE(),
    CONSTRAINT UQ_Musteri_Clean_Email UNIQUE (Email)
);
GO

/* Hatalı kayıtlar tablosu */
CREATE TABLE Musteri_Reject
(
    RejectID INT IDENTITY(1,1) PRIMARY KEY,
    RawID INT NOT NULL,
    Ad NVARCHAR(50),
    Soyad NVARCHAR(50),
    Email NVARCHAR(100),
    Telefon NVARCHAR(30),
    DogumTarihi NVARCHAR(30),
    Sehir NVARCHAR(50),
    HataNedeni NVARCHAR(300),
    RejectTarihi DATETIME DEFAULT GETDATE()
);
GO

/* ETL log tablosu */
CREATE TABLE ETL_Log
(
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    Adim NVARCHAR(100),
    Aciklama NVARCHAR(300),
    KayitSayisi INT,
    IslemTarihi DATETIME DEFAULT GETDATE()
);
GO


/* =========================================================
   3) TEST VERİLERİ
   ========================================================= */

INSERT INTO Musteri_Raw (Ad, Soyad, Email, Telefon, DogumTarihi, Sehir)
VALUES
(' Ahmet ', 'Yılmaz', 'Ahmet@gmail.com', '5321234567', '1990-01-15', ' İstanbul '),
('Ayşe', NULL, 'ayse@gmail', '55512', '01/02/1992', 'Ankara'),
('Mehmet', 'Kara', NULL, '05329998877', '1995-13-01', 'İzmir'),
('Fatma', '', 'fatma@gmail.com', 'abc123456', '1998/12/01', 'Bursa'),
('Ali', 'Demir', 'ali@gmail.com', '5351112233', '1992-05-10', 'Antalya'),
('Ali', 'Demir', 'ALI@gmail.com', '05351112233', '1992-05-10', 'Antalya'),
('Zeynep', 'Çelik', 'zeynep@gmail.com', '5423332211', '2000-11-20', 'İstanbul'),
('Can', 'Arslan', 'canarslan@hotmail.com', '5410009988', '1988-07-09', 'Adana'),
(NULL, 'Ak', 'test@test.com', '5311111111', '1997-03-03', 'Konya'),
('Elif', 'Kurt', 'elif@gmail.com', NULL, '1996-09-15', 'Mersin'),
('Burak', 'Polat', 'burak@gmail.com', '(0532) 444 55 66', '1994-06-22', 'ankara'),
('Deniz', 'Aydın', 'deniz@gmail.com', '0532-777-88-99', '1993-04-11', 'ANKARA'),
('Selin', 'Koç', 'selin@gmail.com', ' 05334445566 ', '1999-08-17', 'izmir');
GO


/* =========================================================
   4) GÖRÜNÜM
   ========================================================= */
CREATE VIEW vw_MusteriRaw
AS
SELECT *
FROM Musteri_Raw;
GO


/* =========================================================
   5) ETL PROCEDURE
   ========================================================= */
CREATE OR ALTER PROCEDURE sp_ETL_MusteriYukle
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('ETL Başlangıç', 'ETL süreci başladı', 0);

        /* Önce staging ve reject temizlenir */
        DELETE FROM Musteri_Staging;
        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('Staging Temizleme', 'Staging tablosu temizlendi', @@ROWCOUNT);

        DELETE FROM Musteri_Reject;
        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('Reject Temizleme', 'Reject tablosu temizlendi', @@ROWCOUNT);

        /* Raw -> Staging */
        INSERT INTO Musteri_Staging
        (
            RawID,
            Ad,
            Soyad,
            Email,
            Telefon,
            DogumTarihi,
            Sehir
        )
        SELECT
            r.RawID,
            NULLIF(LTRIM(RTRIM(r.Ad)), ''),
            ISNULL(NULLIF(LTRIM(RTRIM(r.Soyad)), ''), 'Bilinmiyor'),
            LOWER(NULLIF(LTRIM(RTRIM(r.Email)), '')),
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(r.Telefon, ''))), '(', ''), ')', ''), '-', ''), ' ', ''), '.', ''),
            COALESCE(
                TRY_CONVERT(DATE, r.DogumTarihi, 120),   -- yyyy-mm-dd
                TRY_CONVERT(DATE, r.DogumTarihi, 104),   -- dd.mm.yyyy
                TRY_CONVERT(DATE, r.DogumTarihi, 103),   -- dd/mm/yyyy
                TRY_CONVERT(DATE, r.DogumTarihi, 111),   -- yyyy/mm/dd
                TRY_CONVERT(DATE, r.DogumTarihi)         -- genel deneme
            ),
            CASE
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'ANKARA') THEN N'Ankara'
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'İSTANBUL', N'ISTANBUL') THEN N'İstanbul'
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'İZMİR', N'IZMIR') THEN N'İzmir'
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'BURSA') THEN N'Bursa'
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'ANTALYA') THEN N'Antalya'
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'ADANA') THEN N'Adana'
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'KONYA') THEN N'Konya'
                WHEN UPPER(LTRIM(RTRIM(r.Sehir))) IN (N'MERSİN', N'MERSIN') THEN N'Mersin'
                ELSE LTRIM(RTRIM(r.Sehir))
            END
        FROM Musteri_Raw r;

        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('Staging Yükleme', 'Raw veriler staging tablosuna aktarıldı', @@ROWCOUNT);

        /* Telefon standardizasyonu: 10 haneliyse başına 0 ekle */
        UPDATE Musteri_Staging
        SET Telefon = '0' + Telefon
        WHERE Telefon NOT LIKE '%[^0-9]%'
          AND LEN(Telefon) = 10;

        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('Telefon Standardizasyonu', '10 haneli telefonlara başa 0 eklendi', @@ROWCOUNT);

        /* Hata nedeni yaz */
        ;WITH Duplicates AS
        (
            SELECT
                Email
            FROM Musteri_Staging
            WHERE Email IS NOT NULL
            GROUP BY Email
            HAVING COUNT(*) > 1
        )
        UPDATE s
        SET HataNedeni =
            CASE
                WHEN s.Ad IS NULL THEN N'Ad alanı boş'
                WHEN s.Email IS NULL THEN N'Email alanı boş'
                WHEN s.Email NOT LIKE '%_@_%._%' THEN N'Geçersiz email formatı'
                WHEN s.Telefon IS NULL OR s.Telefon = '' THEN N'Telefon alanı boş'
                WHEN s.Telefon LIKE '%[^0-9]%' THEN N'Telefon sayısal değil'
                WHEN LEN(s.Telefon) <> 11 THEN N'Telefon 11 haneli değil'
                WHEN s.DogumTarihi IS NULL THEN N'Geçersiz doğum tarihi'
                WHEN d.Email IS NOT NULL THEN N'Mükerrer email kaydı'
                ELSE NULL
            END
        FROM Musteri_Staging s
        LEFT JOIN Duplicates d
            ON s.Email = d.Email;

        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('Hata Tespiti', 'Hatalı kayıtlar işaretlendi', @@ROWCOUNT);

        /* Hatalı kayıtları reject tablosuna al */
        INSERT INTO Musteri_Reject
        (
            RawID, Ad, Soyad, Email, Telefon, DogumTarihi, Sehir, HataNedeni
        )
        SELECT
            r.RawID,
            r.Ad,
            r.Soyad,
            r.Email,
            r.Telefon,
            r.DogumTarihi,
            r.Sehir,
            s.HataNedeni
        FROM Musteri_Raw r
        INNER JOIN Musteri_Staging s
            ON r.RawID = s.RawID
        WHERE s.HataNedeni IS NOT NULL;

        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('Reject Aktarım', 'Hatalı kayıtlar reject tablosuna aktarıldı', @@ROWCOUNT);

        /* Geçerli kayıtları clean tabloya aktar */
        INSERT INTO Musteri_Clean
        (
            Ad, Soyad, Email, Telefon, DogumTarihi, Sehir, KaynakRawID
        )
        SELECT
            s.Ad,
            s.Soyad,
            s.Email,
            s.Telefon,
            s.DogumTarihi,
            s.Sehir,
            s.RawID
        FROM Musteri_Staging s
        WHERE s.HataNedeni IS NULL
          AND NOT EXISTS
          (
              SELECT 1
              FROM Musteri_Clean c
              WHERE c.Email = s.Email
          );

        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('Clean Aktarım', 'Geçerli kayıtlar clean tabloya aktarıldı', @@ROWCOUNT);

        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('ETL Bitiş', 'ETL süreci başarıyla tamamlandı', 0);
    END TRY
    BEGIN CATCH
        INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
        VALUES ('HATA', ERROR_MESSAGE(), 0);
    END CATCH
END;
GO


/* =========================================================
   6) INDEXLER
   ========================================================= */
CREATE INDEX IX_Musteri_Staging_RawID
ON Musteri_Staging (RawID);
GO

CREATE INDEX IX_Musteri_Clean_Email
ON Musteri_Clean (Email);
GO

CREATE INDEX IX_Musteri_Reject_RawID
ON Musteri_Reject (RawID);
GO


/* =========================================================
   7) TRIGGER
   ========================================================= */
CREATE OR ALTER TRIGGER trg_MusteriClean_AfterInsert
ON Musteri_Clean
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ETL_Log (Adim, Aciklama, KayitSayisi)
    SELECT
        'Trigger Insert',
        'Musteri_Clean tablosuna yeni kayıt eklendi',
        COUNT(*)
    FROM inserted;
END;
GO


/* =========================================================
   8) PROCEDURE ÇALIŞTIRMA
   ========================================================= */
EXEC sp_ETL_MusteriYukle;
GO


/* =========================================================
   9) RAPORLAR
   ========================================================= */

/* Tüm temiz kayıtlar */
SELECT * FROM Musteri_Clean ORDER BY MusteriID;
GO

/* Tüm hatalı kayıtlar */
SELECT * FROM Musteri_Reject ORDER BY RejectID;
GO

/* ETL logları */
SELECT * FROM ETL_Log ORDER BY LogID;
GO

/* Genel özet raporu */
SELECT
    (SELECT COUNT(*) FROM Musteri_Raw) AS ToplamRawKayit,
    (SELECT COUNT(*) FROM Musteri_Staging) AS ToplamStagingKayit,
    (SELECT COUNT(*) FROM Musteri_Clean) AS TemizKayitSayisi,
    (SELECT COUNT(*) FROM Musteri_Reject) AS HataliKayitSayisi;
GO

/* Hata nedenine göre dağılım */
SELECT
    HataNedeni,
    COUNT(*) AS KayitSayisi
FROM Musteri_Reject
GROUP BY HataNedeni
ORDER BY KayitSayisi DESC;
GO

/* Şehre göre temiz müşteri dağılımı */
SELECT
    Sehir,
    COUNT(*) AS MusteriSayisi
FROM Musteri_Clean
GROUP BY Sehir
ORDER BY MusteriSayisi DESC;
GO

/* Email problemi olan kayıt sayısı */
SELECT
    COUNT(*) AS GecersizEmailSayisi
FROM Musteri_Staging
WHERE Email IS NULL
   OR Email NOT LIKE '%_@_%._%';
GO

/* Telefon problemi olan kayıt sayısı */
SELECT
    COUNT(*) AS GecersizTelefonSayisi
FROM Musteri_Staging
WHERE Telefon IS NULL
   OR Telefon LIKE '%[^0-9]%'
   OR LEN(Telefon) <> 11;
GO

/* Doğum tarihi problemi olan kayıt sayısı */
SELECT
    COUNT(*) AS GecersizDogumTarihiSayisi
FROM Musteri_Staging
WHERE DogumTarihi IS NULL;
GO

/* Temizleme öncesi / sonrası karşılaştırma */
SELECT
    'Raw Veri' AS TabloAdi, COUNT(*) AS KayitSayisi
FROM Musteri_Raw
UNION ALL
SELECT
    'Clean Veri', COUNT(*)
FROM Musteri_Clean
UNION ALL
SELECT
    'Reject Veri', COUNT(*)
FROM Musteri_Reject;
GO
