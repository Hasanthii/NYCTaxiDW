USE NYCTaxiDW;
GO

INSERT INTO dim_vendor (vendor_key, vendor_name) VALUES
(1, 'Creative Mobile Technologies, LLC'),
(2, 'Curb Mobility, LLC'),
(6, 'Myle Technologies Inc'),
(7, 'Helix');

INSERT INTO dim_rate_code (rate_code_key, rate_code_name) VALUES
(1,  'Standard rate'),
(2,  'JFK'),
(3,  'Newark'),
(4,  'Nassau or Westchester'),
(5,  'Negotiated fare'),
(6,  'Group ride'),
(99, 'Unknown');

INSERT INTO dim_payment_type (payment_type_key, payment_type_name) VALUES
(0, 'Flex fare trip'),
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip');
GO