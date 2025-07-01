export interface PoliticianFinanceProps {
  politicianId: string;
  usedParliamentaryQuota: number;
  unusedParliamentaryQuota: number;
  usedCabinetQuota: number;
  unusedCabinetQuota: number;
  contractedPeople: string;
  contractedPeopleUrl: string;
  grossSalary: string;
  functionalPropertyUsage: string;
  housingAssistant: string;
  diplomaticPassport: string;
  trips: string;
  year: number;
}

export interface PoliticianMonthlyCostProps {
  politicianId: string;
  politicianFinanceId: string;
  parliamentaryQuota: number;
  cabinetQuota: number;
  month: number;
  year: number;
}

export interface PoliticianPositionProps {
  politicianId: string;
  year: number;
  position: string;
  startDate: string;
  name: string;
}

export interface PoliticianProfileProps {
  politicianId: string;
  createdProposals: number;
  relatedProposals: number;
  speeches: number;
  year: number;
  rollCallVotes: number;
  createdProposalsUrl: string;
  relatedProposalsUrl: string;
  speechesVideosUrl: string;
  speechesAudiosUrl: string;
  rollCallVotesUrl: string;
  plenaryPresence: number;
  plenaryJustifiedAbsences: number;
  plenaryUnjustifiedAbsences: number;
  committeesPresence: number;
  commissions: string;
  committeesJustifiedAbsences: number;
  committeesUnjustifiedAbsences: number;
}
