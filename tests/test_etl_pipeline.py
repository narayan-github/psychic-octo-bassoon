"""
Test suite for Healthcare Claims ETL Pipeline
"""

import pytest
from etl_pipeline import HealthcareETL


class TestHealthcareETL:
    """Test cases for HealthcareETL class"""

    @pytest.fixture
    def etl(self):
        """Create a HealthcareETL instance for testing"""
        return HealthcareETL()

    def test_initialization(self, etl):
        """Test that ETL pipeline initializes correctly"""
        assert etl.db_config is not None
        assert 'host' in etl.db_config
        assert 'database' in etl.db_config
        assert 'user' in etl.db_config
        assert 'password' in etl.db_config

    def test_extract_claims(self, etl):
        """Test claims extraction returns expected data structure"""
        claims = etl.extract_claims()

        assert isinstance(claims, list)
        assert len(claims) > 0

        # Verify structure of first claim
        first_claim = claims[0]
        assert 'claim_id' in first_claim
        assert 'patient_id' in first_claim
        assert 'provider_id' in first_claim
        assert 'service_date' in first_claim
        assert 'procedure_code' in first_claim
        assert 'amount' in first_claim
        assert 'status' in first_claim

    def test_extract_claims_returns_multiple_claims(self, etl):
        """Test that extraction returns multiple claims"""
        claims = etl.extract_claims()
        assert len(claims) >= 3

    def test_transform_claims(self, etl):
        """Test claims transformation"""
        raw_claims = etl.extract_claims()
        transformed_claims = etl.transform_claims(raw_claims)
        
        assert isinstance(transformed_claims, list)
        assert len(transformed_claims) == len(raw_claims)

        # Verify transformation added new fields
        first_transformed = transformed_claims[0]
        assert 'processed_date' in first_transformed
        assert 'is_valid' in first_transformed
        status_upper = first_transformed['status'].upper()
        assert first_transformed['status'] == status_upper

    def test_validate_claim_valid(self, etl):
        """Test validation of a valid claim"""
        valid_claim = {
            'claim_id': 'CLM001',
            'procedure_code': '99213',
            'amount': 150.00
        }
        assert etl._validate_claim(valid_claim) is True

    def test_validate_claim_missing_claim_id(self, etl):
        """Test validation fails when claim_id is missing"""
        invalid_claim = {
            'procedure_code': '99213',
            'amount': 150.00
        }
        assert etl._validate_claim(invalid_claim) is False

    def test_validate_claim_missing_procedure_code(self, etl):
        """Test validation fails when procedure_code is missing"""
        invalid_claim = {
            'claim_id': 'CLM001',
            'amount': 150.00
        }
        assert etl._validate_claim(invalid_claim) is False

    def test_validate_claim_invalid_amount(self, etl):
        """Test validation fails when amount is zero or negative"""
        invalid_claim = {
            'claim_id': 'CLM001',
            'procedure_code': '99213',
            'amount': 0
        }
        assert etl._validate_claim(invalid_claim) is False

        invalid_claim_negative = {
            'claim_id': 'CLM001',
            'procedure_code': '99213',
            'amount': -50.00
        }
        assert etl._validate_claim(invalid_claim_negative) is False

    def test_validate_claim_empty_claim_id(self, etl):
        """Test validation fails when claim_id is empty string"""
        invalid_claim = {
            'claim_id': '',
            'procedure_code': '99213',
            'amount': 150.00
        }
        assert etl._validate_claim(invalid_claim) is False

    def test_load_claims_success(self, etl):
        """Test successful loading of claims"""
        claims = etl.extract_claims()
        transformed_claims = etl.transform_claims(claims)
        result = etl.load_claims(transformed_claims)
        assert result is True

    def test_load_claims_empty_list(self, etl):
        """Test loading empty claims list"""
        result = etl.load_claims([])
        assert result is True

    def test_run_pipeline_complete_flow(self, etl):
        """Test complete pipeline execution"""
        results = etl.run_pipeline()

        assert isinstance(results, dict)
        assert 'extracted' in results
        assert 'transformed' in results
        assert 'loaded' in results
        assert 'success' in results
        assert results['extracted'] > 0
        assert results['transformed'] > 0
        assert results['loaded'] > 0
        assert results['success'] is True

    def test_run_pipeline_counts_match(self, etl):
        """Test that extracted, transformed, and loaded counts match"""
        results = etl.run_pipeline()
        assert (
            results['extracted'] == results['transformed'] == results['loaded']
        )

    def test_transform_preserves_claim_ids(self, etl):
        """Test that transformation preserves claim IDs"""
        raw_claims = etl.extract_claims()
        transformed_claims = etl.transform_claims(raw_claims)

        raw_ids = [claim['claim_id'] for claim in raw_claims]
        transformed_ids = [claim['claim_id'] for claim in transformed_claims]

        assert raw_ids == transformed_ids

    def test_transform_validates_claims(self, etl):
        """Test that transformation marks claims as valid/invalid"""
        raw_claims = etl.extract_claims()
        transformed_claims = etl.transform_claims(raw_claims)

        # All sample claims should be valid
        for claim in transformed_claims:
            assert 'is_valid' in claim
            assert isinstance(claim['is_valid'], bool)

    def test_transform_adds_processed_date(self, etl):
        """Test that transformation adds processed date"""
        raw_claims = etl.extract_claims()
        transformed_claims = etl.transform_claims(raw_claims)

        for claim in transformed_claims:
            assert 'processed_date' in claim
            assert claim['processed_date'] == '2024-01-20'

    def test_extract_returns_dict_values(self, etl):
        """Test that extraction returns proper data types"""
        claims = etl.extract_claims()

        for claim in claims:
            assert isinstance(claim, dict)
            assert isinstance(claim['claim_id'], str)
            assert isinstance(claim['patient_id'], str)
            assert isinstance(claim['provider_id'], str)
            assert isinstance(claim['service_date'], str)
            assert isinstance(claim['procedure_code'], str)
            assert isinstance(claim['amount'], (int, float))
            assert isinstance(claim['status'], str)

    def test_transform_uppercases_status(self, etl):
        """Test that transformation converts status to uppercase"""
        raw_claims = etl.extract_claims()
        transformed_claims = etl.transform_claims(raw_claims)

        for claim in transformed_claims:
            assert claim['status'].isupper()
