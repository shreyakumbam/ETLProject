INSERT INTO etl_data (
    SourceDocumentID, DataDescription, FileName, Title, QCScanDate, CMODLoadDate, ScanDate, DocumentID, BatchReferenceID, BatchClassName) 
VALUES
(1001, 
'Invoice from Vendor A for the July 2023 billing period. This invoice includes itemized charges for multiple goods delivered during the month. Each item has been verified for price accuracy and quantity. The invoice references Purchase Order #PO4567 and includes payment terms of Net 30. Taxes and shipping fees are listed separately. This invoice was digitally approved by the procurement manager and uploaded by the finance team. The scan passed OCR quality checks and has been tagged for indexing. Summary: Vendor ID 874, PO #PO4567, Total $4,576.34. Additional metadata includes the delivery address, invoice issue date, and payment reference number. This document will be used for three-way matching during payment processing. The invoice is stored in the July_2023_finance_docs repository. Verified and filed on 2023-07-02 by finance-admin. Payment expected by 2023-08-01. File includes electronic signature and reference barcode. Internal notes: This vendor has had no issues in prior audits. PDF generated from scanned image with searchable text layer. File checksum recorded for audit purposes. Related documents include packing slip and delivery confirmation.',
'invoice_1001.pdf', 'Invoice July 2023', '2023-07-01', '2023-07-02', '2023-06-30', 'DOC1001', 501, 'InvoiceBatch'),

(1002, 
'Receipt for office supplies purchased by Facilities Team in July 2023. This physical receipt was scanned at high resolution and saved in the financial repository for record-keeping and audit trail creation. Items listed include printer paper, ink cartridges, staplers, and label rolls. Each line item includes a SKU, unit price, quantity, and subtotal. Total amount paid: $183.47. Payment method: Corporate credit card ending in 8421. The receipt was signed by the store manager and includes a transaction number for reference. It was approved by the department head and flagged as compliant with procurement policy. Metadata fields include store name, employee name, project code, and account code for expense attribution. The document was digitally signed by the supervisor and passed through OCR text extraction. This scan is part of batch #REC202307_Procurement. Receipt validated on 2023-07-06 and attached to the department’s monthly expenditure report. Internal flag indicates no issues found. Useful for monthly reconciliation, tax audits, and vendor performance tracking. Image clarity level: High.',
'receipt_202307.pdf', 'July Receipt', '2023-07-05', '2023-07-06', '2023-07-04', 'DOC1002', 502, 'ReceiptBatch'),

(1003, 
'Shipping manifest for shipment #56789, processed on 2023-07-07 by the logistics center. This manifest outlines 14 pallets of mixed merchandise, including electronics, textiles, and hardware parts. Each row in the manifest includes SKU, quantity, weight, and destination location. The total shipment weight is 3,486 lbs. The file includes carrier assignment (ABC Logistics), departure timestamp, and expected delivery ETA of 2023-07-10. The manifest was signed off by the dock supervisor and is used for cross-verification at delivery. Items were scanned and barcoded during loading. Scan data was uploaded into the logistics dashboard for real-time tracking. File has been reviewed for accuracy and archived in the shipping database. Manifest matches PO #87923 and is linked to Warehouse Transfer Order #WTO567. System flags indicate on-time dispatch and no reported delays. The document was scanned with barcode overlays and passed pre-upload QA. This version is final and submitted to the compliance folder. Useful for freight audit, damage claims, and route optimization studies. Logged under 2023_Q3_Shipping.',
'ship_manifest.pdf', 'Shipment #56789', '2023-07-08', '2023-07-09', '2023-07-07', 'DOC1003', 503, 'ShippingBatch'),

(1004, 
'Q2 Quality Report for 2023 compiled from inspections conducted at all four production facilities. The report summarizes key quality metrics including defect rates, compliance percentages, and corrective actions taken. Areas reviewed include inbound material checks, in-process controls, and final inspection pass rates. Charts show month-over-month improvements across three main product lines. Notable achievements include 98.5% compliance for packaging, a 12% reduction in defects, and successful implementation of new SOPs. Each finding is supported by data collected via the quality dashboard. The report includes signatures from the QA Lead and the VP of Manufacturing. This document is used for internal review, supplier scorecards, and ISO 9001 compliance audits. Filed under quality_docs/Q2_2023 and tagged by business unit. Internal notes confirm the document was peer-reviewed and locked for edits. Observations highlight training improvements and reduced rework time. All reported issues were addressed with CAPA documentation attached. This PDF has OCR layers for accessibility and is stored with associated XML metadata.',
'quality_q2.pdf', 'Q2 Quality Report', '2023-07-10', '2023-07-11', '2023-07-09', 'DOC1004', 504, 'QualityBatch'),

(1005, 
'Packing list generated for shipment #56789 detailing all items included in the dispatch. Document includes 128 line items with SKUs, item descriptions, quantity per carton, weight, dimensions, and special handling instructions. Generated automatically via ERP system at 06:23 AM on 2023-07-11. Reviewed and approved by Warehouse Manager prior to outbound scan. Pallet ID ranges PLT-402 to PLT-415. Contains grouped product types categorized for destination hubs. Internal notes show carton seals were inspected and verified. The file is signed digitally by two team leads and checked into the logistics archive. Barcode page was appended to the end for quick validation. This packing list is part of a bundled shipment tagged to OrderGroup_778_AugustWave. The list has been matched against the outbound shipping manifest. Timestamps, delivery region tags, and picker IDs included. Document passed integrity check and OCR was verified. Archived at 2023_logistics/batch56789 for downstream review and audit. Reference tracking ID: LTS789003214.',

'packing_56789.pdf', 'Packing List 56789', '2023-07-12', '2023-07-13', '2023-07-11', 'DOC1005', 505, 'LogisticsBatch');


SELECT * FROM loader_table;

CREATE EXTENSION IF NOT EXISTS vector;

DROP TABLE IF EXISTS etl_embeddings;

CREATE TABLE etl_embeddings (
    SourceDocumentID INTEGER PRIMARY KEY,
    DataDescription TEXT,
    Embedding vector(768)
);

SELECT * FROM etl_embeddings;
