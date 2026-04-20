from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foundation_intel.build_dataset import parse_qualifying_grants


def test_parse_qualifying_grants_matches_recipient_and_purpose_keywords():
    xml = b'''<?xml version="1.0" encoding="utf-8"?>
    <Return xmlns="http://www.irs.gov/efile">
      <ReturnData>
        <IRS990PF>
          <SupplementaryInformationGrp>
            <GrantOrContributionPdDurYrGrp>
              <RecipientBusinessName>
                <BusinessNameLine1Txt>Example University</BusinessNameLine1Txt>
              </RecipientBusinessName>
              <RecipientUSAddress>
                <CityNm>Syracuse</CityNm>
                <StateAbbreviationCd>NY</StateAbbreviationCd>
                <ZIPCd>13244</ZIPCd>
              </RecipientUSAddress>
              <RecipientRelationshipTxt>NONE</RecipientRelationshipTxt>
              <RecipientFoundationStatusTxt>PC</RecipientFoundationStatusTxt>
              <GrantOrContributionPurposeTxt>Scholarship support</GrantOrContributionPurposeTxt>
              <Amt>50000</Amt>
            </GrantOrContributionPdDurYrGrp>
          </SupplementaryInformationGrp>
        </IRS990PF>
      </ReturnData>
    </Return>
    '''

    grants = parse_qualifying_grants(xml)

    assert len(grants) == 1
    assert grants[0]["grant_recipient_name"] == "Example University"
    assert grants[0]["grant_amount_usd"] == "50000"
    assert "recipient_name_keyword" in grants[0]["higher_ed_match_basis"]
    assert "purpose_keyword" in grants[0]["higher_ed_match_basis"]


def test_parse_qualifying_grants_filters_non_matching_records():
    xml = b'''<?xml version="1.0" encoding="utf-8"?>
    <Return xmlns="http://www.irs.gov/efile">
      <ReturnData>
        <IRS990PF>
          <SupplementaryInformationGrp>
            <GrantOrContributionPdDurYrGrp>
              <RecipientBusinessName>
                <BusinessNameLine1Txt>Neighborhood Arts Collective</BusinessNameLine1Txt>
              </RecipientBusinessName>
              <GrantOrContributionPurposeTxt>General operating support</GrantOrContributionPurposeTxt>
              <Amt>15000</Amt>
            </GrantOrContributionPdDurYrGrp>
          </SupplementaryInformationGrp>
        </IRS990PF>
      </ReturnData>
    </Return>
    '''

    grants = parse_qualifying_grants(xml)

    assert grants == []
