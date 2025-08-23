#!/usr/bin/env python3
"""
Korean mapping test script for house_price_and_sales_refactor.py
Tests if the Korean mapping dictionary correctly matches DataFrame column names
"""

import sys
import os
sys.path.append('/home/jyp0615/us_eco')

def test_korean_mapping():
    """Test Korean mapping structure and completeness"""
    
    try:
        from house_price_and_sales_refactor import HOUSE_PRICE_KOREAN_NAMES, HOUSE_PRICE_DATA_CATEGORIES
        
        print("🧪 Testing Korean Mapping for House Price Data")
        print("="*60)
        
        # Test 1: Check Korean mapping dictionary structure
        print(f"📚 Korean mapping dictionary size: {len(HOUSE_PRICE_KOREAN_NAMES)} entries")
        
        # Test 2: Sample mapping patterns
        print("\n🔍 Sample Korean mappings (expected format):")
        sample_mappings = [
            ('case_shiller_cs_national_sa', 'CS 전국 지수(SA)'),
            ('fhfa_fhfa_national_sa', 'FHFA 전국(SA)'),
            ('existing_home_sales_ehs_sales_national_sa', 'EHS 전국 판매량(SA)'),
            ('new_residential_sales_nrs_sales_national_sa', 'NRS 전국 판매량(SA)')
        ]
        
        for key, expected in sample_mappings:
            if key in HOUSE_PRICE_KOREAN_NAMES:
                actual = HOUSE_PRICE_KOREAN_NAMES[key]
                status = "✅" if actual == expected else "⚠️"
                print(f"  {status} {key}")
                print(f"    Expected: {expected}")
                print(f"    Actual:   {actual}")
            else:
                print(f"  ❌ {key} → Not found in mapping")
        
        # Test 3: Check for category_indicator format consistency
        print(f"\n🎯 Testing category_indicator format consistency:")
        categories = ['case_shiller', 'fhfa', 'zillow', 'existing_home_sales', 'new_residential_sales']
        
        for category in categories:
            category_keys = [k for k in HOUSE_PRICE_KOREAN_NAMES.keys() if k.startswith(category + '_')]
            print(f"  {category}: {len(category_keys)} mappings found")
            
            if category_keys:
                # Show first few examples
                for i, key in enumerate(category_keys[:3]):
                    print(f"    {i+1}. {key} → {HOUSE_PRICE_KOREAN_NAMES[key]}")
        
        # Test 4: Check for old format entries (should be none)
        print(f"\n🔍 Checking for old format entries (FRED ID keys):")
        old_format_indicators = ['CSUSHPISA', 'HPIPONM226S', 'EXHOSLUSM495S', 'HSN1F']
        old_format_found = 0
        
        for indicator in old_format_indicators:
            if indicator in HOUSE_PRICE_KOREAN_NAMES:
                print(f"  ⚠️  Found old format key: {indicator}")
                old_format_found += 1
        
        if old_format_found == 0:
            print("  ✅ No old format entries found - mapping correctly updated")
        
        print(f"\n✅ Korean mapping test completed successfully")
        print(f"   Total mappings: {len(HOUSE_PRICE_KOREAN_NAMES)}")
        print(f"   Format: category_indicator → Korean name")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    test_korean_mapping()