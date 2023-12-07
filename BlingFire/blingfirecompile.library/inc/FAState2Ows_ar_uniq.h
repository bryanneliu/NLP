/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */


#ifndef _FA_STATE2OWS_AR_UNIQ_H_
#define _FA_STATE2OWS_AR_UNIQ_H_

#include "FAConfig.h"
#include "FAState2OwsA.h"
#include "FAMultiMap_ar_uniq.h"

namespace BlingFire
{

class FAAllocatorA;

///
/// separate implementation of FAState2OwsA
///

class FAState2Ows_ar_uniq : public FAState2OwsA {

public:
    FAState2Ows_ar_uniq (FAAllocatorA * pAlloc);
    virtual ~FAState2Ows_ar_uniq ();

public:
    const int GetOws (const int State, const int ** ppOws) const;
    const int GetOws (const int State, int * pOws, const int MaxCount) const;
    const int GetMaxOwsCount () const;

    void SetOws (const int State, const int * pOws, const int Size);
    void Clear ();

private:
    /// Max OwsSize
    int m_MaxOwsCount;
    /// maps State to Ows
    FAMultiMap_ar_uniq m_state2ows;
};

}

#endif
