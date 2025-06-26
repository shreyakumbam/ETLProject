CREATE TABLE etl_data (
    SourceDocumentID INTEGER,
	DataDescription VARCHAR(300),
    FileName VARCHAR(256),
    Title VARCHAR(100),
    QCScanDate DATE,
    CMODLoadDate DATE,
    ScanDate DATE,
    DocumentID VARCHAR(100),
    BatchReferenceID INTEGER,
    BatchClassName TEXT
);

DROP TABLE etl_data;

INSERT INTO etl_data (
    SourceDocumentID, DataDescription, FileName, Title, QCScanDate, CMODLoadDate, ScanDate, DocumentID, BatchReferenceID, BatchClassName) 
VALUES
(1001, 'Invoice from vendor A', 'invoice_1001.pdf', 'Invoice July 2023', '2023-07-01', '2023-07-02', '2023-06-30', 'DOC1001', 501, 'InvoiceBatch'),
(1002, 'Receipt scan', 'receipt_202307.pdf', 'July Receipt', '2023-07-05', '2023-07-06', '2023-07-04', 'DOC1002', 502, 'ReceiptBatch'),
(1003, 'Shipping manifest', 'ship_manifest.pdf', 'Shipment #56789', '2023-07-08', '2023-07-09', '2023-07-07', 'DOC1003', 503, 'ShippingBatch'),
(1004, 'Quality report', 'quality_q2.pdf', 'Q2 Quality Report', '2023-07-10', '2023-07-11', '2023-07-09', 'DOC1004', 504, 'QualityBatch'),
(1005, 'Packing list', 'packing_56789.pdf', 'Packing List 56789', '2023-07-12', '2023-07-13', '2023-07-11', 'DOC1005', 505, 'LogisticsBatch');




UPDATE etl_data SET source = null 
WHERE sourcebatchid = 'SB002';

SELECT * FROM etl_data;