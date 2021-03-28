bill_template = """<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <link rel="icon" href="{{client_logo}}" id="light-scheme-icon">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>{{client_name}} - {{invoice_no}}</title>
    <style type="text/css">
      * {
        font-family: 'Poppins', sans-serif, Helvetica, Arial !important;
      }
      body {
        width: 100%;
      }
      .page-container {
        display: flex;
        flex-direction: column;
        padding: 30px;
        max-width: 100%;
        width: 600px;
        margin: 0 auto;
      }
      .seller-logo img {
        max-width: 100%;
        height: auto;
        width: 400px;
        display: block;
        margin: 0 auto;
      }
      .seller-info {
        margin-top: 40px;
        color: #212121;
        font-size: 20px;
        display: flex;
        flex-direction: column;
        align-items: center;
      }
      .seller-name {
        font-weight: 600;
        padding: 10px 0;
        font-size: 24px;
      }
      .seller-gst label {
        font-weight: 600;
      }
      .invoice-details {
        padding-top: 40px;
        display: flex;
        flex-wrap: wrap;
        justify-content: space-between;
      }
      .invoice-details table {
        max-width:100%; 
        margin: auto; 
        text-align: center;
      }
      .invoice-details .detail {
        padding-bottom: 30px;
        font-size: 20px;
      }
      .invoice-details .detail label {
        font-weight: 600;
      }
      .invoice-details .detail div {
        padding-top: 10px;
      }
      @media (max-width: 575px) {
        .invoice-details table {
          text-align: left;
        }
      }
    </style>
  </head>
  <body>
    <div class="page-container">
      <div class="seller-logo">
        <img src="{{client_logo}}" alt="{{client_name}}"/>
      </div>
      <div class="seller-info">
        <div class="seller-name">
          {{client_name}}
        </div>
        <div class="seller-gst">
          <label>GSTIN: </label>
          <span>{{client_gstin}}</span>
        </div>
      </div>
      <div class="invoice-details">
        <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%">
          <tr>
            <td>
              <div class="detail">
                <label>Invoice Number</label>
                <div>{{invoice_no}}</div>
              </div>
            </td>
            <td>
              <div class="detail">
                <label>Invoice Date</label>
                <div>{{invoice_date}}</div>
              </div>
            </td>
          </tr>
          <tr>
            <td>
              <div class="detail">
                <label>CGST</label>
                <div>{{invoice_cgst}}</div>
              </div>
            </td>
            <td>
              <div class="detail">
                <label>SGST</label>
                <div>{{invoice_sgst}}</div>
              </div>
            </td>
          </tr>
          <tr>
            <td>
              <div class="detail">
                <label>IGST</label>
                <div>{{invoice_igst}}</div>
              </div>
            </td>
            <td>
              <div class="detail">
                <label>Total Amount</label>
                <div>{{invoice_amount}}</div>
              </div>
            </td>
          </tr>
        </table>
      </div>
    </div>
  </body>
</html>"""