# ![Polars Bloomberg Logo](assets/polars_bloomberg_logo.png)

# Polars + Bloomberg Open API
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Python library providing a Polars DataFrame interface for easy and intuitive access to the Bloomberg Open API

# Features
- Seamless integration with Polars DataFrames
- Excel like inputs
- Polars DataFrame outputs
- No dependency on Pandas

# Usage
```python
from polars_bloomberg import BQuery

with BQuery() as bq:
    df_ref = bq.bdp(['AAPL US Equity', 'MSFT US Equity'], ['PX_LAST'])
    df_hist = bq.bdh(['AAPL US Equity'], ['PX_LAST'], date(2020, 1, 1), date(2020, 1, 30))
```


# Disclaimer
<small>
**Important:** Please read this disclaimer carefully before using the **polars-bloomberg** library.

### 1. No Affiliation with Bloomberg L.P.

*polars-bloomberg* is an independent, open-source project and is **not affiliated with**, **endorsed by**, or **in any way connected to** Bloomberg L.P.. This project is developed independently by the open-source community and is not sanctioned by Bloomberg L.P.

### 2. Bloomberg License Requirement

Accessing Bloomberg data through the *polars-bloomberg* library **requires a valid Bloomberg license**. Users must ensure they have the necessary permissions and licenses from Bloomberg L.P. to utilize their APIs and data services. The *polars-bloomberg* project does not provide or facilitate the acquisition of Bloomberg licenses.

### 3. No Warranty and Limitation of Liability

The *polars-bloomberg* library is provided **"as is"**, without any express or implied warranties, including, but not limited to, warranties of merchantability, fitness for a particular purpose, and non-infringement. In no event shall the contributors or maintainers of *polars-bloomberg* be liable for any direct, indirect, incidental, special, exemplary, or consequential damages (including, but not limited to, procurement of substitute goods or services; loss of use, data, or profits; or business interruption) however caused and on any theory of liability, whether in contract, strict liability, or tort (including negligence or otherwise) arising in any way out of the use of this library, even if advised of the possibility of such damage.

### 4. No Financial Advice

The *polars-bloomberg* library is intended solely for informational and analytical purposes. It does **not constitute financial advice**, investment advice, trading advice, or any other sort of advice. Users should consult with a qualified financial advisor or other professional before making any financial decisions based on the data or analyses obtained through this tool.

### 5. Compliance with Laws and Regulations

Users are responsible for ensuring that their use of the *polars-bloomberg* library complies with all applicable local, state, national, and international laws and regulations. The developers and contributors of *polars-bloomberg* are not responsible for any unlawful or unauthorized use of the library.

### 6. Data Accuracy and Reliability

While every effort is made to ensure the accuracy and reliability of the data accessed and processed through the *polars-bloomberg* library, the developers and contributors do **not warrant** that the data is complete, accurate, or up-to-date. Users should independently verify the data and analyses before making any decisions based on them.

### 7. Indemnification

By using the *polars-bloomberg* library, you agree to indemnify, defend, and hold harmless the contributors, maintainers, and any affiliated parties from and against any and all claims, damages, obligations, losses, liabilities, costs, or debt, and expenses (including but not limited to attorney's fees) arising from:
- Your use of and access to the *polars-bloomberg* library;
- Your violation of any term of this disclaimer;
- Your violation of any third-party right, including without limitation any copyright, property, or privacy right;
- Any claim that your use of the library caused damage to a third party.

### 8. Changes to This Disclaimer

The developers and contributors of *polars-bloomberg* reserve the right to modify or replace this disclaimer at any time. It is your responsibility to check this disclaimer periodically for changes. Your continued use of the library following the posting of any changes to this disclaimer constitutes acceptance of those changes.
</small>

**By using the *polars-bloomberg* library, you acknowledge that you have read, understood, and agree to be bound by this disclaimer. If you do not agree with any part of this disclaimer, you must not use the library.**
