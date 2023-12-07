/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */


#ifndef _FA_ENCODER_DELTA_PREF_H_
#define _FA_ENCODER_DELTA_PREF_H_

#include "FAConfig.h"
#include "FAEncoderA.h"

namespace BlingFire
{

/// Does the following:
/// 1. Calculates delta between next Element of the sequence and the previous.
/// 2. Makes preffix encoding as FAEncoder_pref class do.

class FAEncoder_delta_pref : public FAEncoderA {

public:

  const int GetMaxBytes () const;
  const int Encode (const int * pChain,
                    const int Size,
                    unsigned char * pOutBuffer) const;

private:

  enum { MAX_BYTES_PER_INT = 5 };

};

}

#endif
