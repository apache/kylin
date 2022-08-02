--
-- Copyright (C) 2020 Kyligence Inc. All rights reserved.
--
-- http://kyligence.io
--
-- This software is the confidential and proprietary information of
-- Kyligence Inc. ("Confidential Information"). You shall not disclose
-- such Confidential Information and shall use it only in accordance
-- with the terms of the license agreement you entered into with
-- Kyligence Inc.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
-- A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
-- OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
-- SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
-- LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
-- THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
-- (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--
select count(distinct TO_TIMESTAMP("UPD_DATE")) "NoFmt",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm:ss')) "yyyy-MM-dd HH:mm:ss",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH:mm')) "yyyy-MM-dd HH:mm",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd HH')) "yyyy-MM-dd HH",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM-dd')) "yyyy-MM-dd",
       count(distinct To_Timestamp(UPD_DATE,'yyyy-MM')) "yyyy-MM",
       count(distinct To_Timestamp(UPD_DATE,'yyyy')) "yyyy"
from TEST_CATEGORY_GROUPINGS