order_shipped = """<!-- THIS EMAIL WAS BUILT AND TESTED WITH LITMUS http://litmus.com -->
<!-- IT WAS RELEASED UNDER THE MIT LICENSE https://opensource.org/licenses/MIT -->
<!-- QUESTIONS? TWEET US @LITMUSAPP -->
<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml"
  xmlns:o="urn:schemas-microsoft-com:office:office">

<head>
  <meta charset="utf-8"> <!-- utf-8 works for most cases -->
  <meta name="viewport" content="width=device-width"> <!-- Forcing initial-scale shouldn't be necessary -->
  <meta http-equiv="X-UA-Compatible" content="IE=edge"> <!-- Use the latest (edge) version of IE rendering engine -->
  <meta name="x-apple-disable-message-reformatting"> <!-- Disable auto-scale in iOS 10 Mail entirely -->
  <link href="https://fonts.googleapis.com/css?family=Poppins:thin,thinItalic,extra-light,extra-lightItalic,regular,regularItalic,medium,mediumItalic,semi-bold,semi-boldItalic,bold,boldItalic" rel="stylesheet">
  <style type="text/css">
    * {
      font-family: 'Poppins', sans-serif, Helvetica, Arial !important;
    }
  </style>

  <!-- Web Font / @font-face : BEGIN -->
  <!-- NOTE: If web fonts are not required, lines 10 - 27 can be safely removed. -->

  <!-- Desktop Outlook chokes on web font references and defaults to Times New Roman, so we force a safe fallback font. -->
  <!--[if mso]>
        <style>
            * {
                font-family: 'Poppins', sans-serif, Helvetica, Arial !important;
            }
        </style>
    <![endif]-->

  <!-- All other clients get the webfont reference; some will render the font and others will silently fail to the fallbacks. More on that here: http://stylecampaign.com/blog/2015/02/webfont-support-in-email/ -->
  <!--[if !mso]><!-->
  <!--<![endif]-->

  <!-- Web Font / @font-face : END -->

  <!-- CSS Reset -->
  <style>
    /* What it does: Remove spaces around the email design added by some email clients. */
    /* Beware: It can remove the padding / margin and add a background color to the compose a reply window. */
    html,
    body {
      margin: 0 auto !important;
      padding: 0 !important;
      height: 100% !important;
      width: 100% !important;
      font-family: 'Poppins', sans-serif, Helvetica, Arial !important;
    }

    /* What it does: Stops email clients resizing small text. */
    * {
      -ms-text-size-adjust: 100%;
      -webkit-text-size-adjust: 100%;
    }

    /* What it does: Centers email on Android 4.4 */
    div[style*="margin: 16px 0"] {
      margin: 0 !important;
    }

    /* What it does: Stops Outlook from adding extra spacing to tables. */
    table,
    td {
      mso-table-lspace: 0pt !important;
      mso-table-rspace: 0pt !important;
    }

    /* What it does: Fixes webkit padding issue. Fix for Yahoo mail table alignment bug. Applies table-layout to the first 2 tables then removes for anything nested deeper. */
    table {
      border-spacing: 0 !important;
      border-collapse: collapse !important;
      table-layout: fixed !important;
      margin: 0 auto !important;
    }

    table table table {
      table-layout: auto;
    }

    /* What it does: Uses a better rendering method when resizing images in IE. */
    img {
      -ms-interpolation-mode: bicubic;
    }

    /* What it does: A work-around for email clients meddling in triggered links. */
    *[x-apple-data-detectors],
    /* iOS */
    .x-gmail-data-detectors,
    /* Gmail */
    .x-gmail-data-detectors *,
    .aBn {
      border-bottom: 0 !important;
      cursor: default !important;
      color: inherit !important;
      text-decoration: none !important;
      font-size: inherit !important;
      font-family: inherit !important;
      font-weight: inherit !important;
      line-height: inherit !important;
    }

    /* What it does: Prevents Gmail from displaying an download button on large, non-linked images. */
    .a6S {
      display: none !important;
      opacity: 0.01 !important;
    }

    /* If the above doesn't work, add a .g-img class to any image in question. */
    img.g-img+div {
      display: none !important;
    }

    /* What it does: Prevents underlining the button text in Windows 10 */
    .button-link {
      text-decoration: none !important;
    }

    /* What it does: Removes right gutter in Gmail iOS app: https://github.com/TedGoas/Cerberus/issues/89  */
    /* Create one of these media queries for each additional viewport size you'd like to fix */
    /* Thanks to Eric Lepetit @ericlepetitsf) for help troubleshooting */
    @media only screen and (min-device-width: 375px) and (max-device-width: 413px) {

      /* iPhone 6 and 6+ */
      .email-container {
        min-width: 375px !important;
      }
    }
  </style>

  <!-- Progressive Enhancements -->
  <style>
    /* What it does: Hover styles for buttons */
    .button-td,
    .button-a {
      transition: all 100ms ease-in;
    }

    .button-td:hover,
    .button-a:hover {
      background-position: right center;
    }

    /* Media Queries */
    @media screen and (max-width: 480px) {

      /* What it does: Forces elements to resize to the full width of their container. Useful for resizing images beyond their max-width. */
      .fluid {
        width: 100% !important;
        max-width: 100% !important;
        height: auto !important;
        margin-left: auto !important;
        margin-right: auto !important;
      }

      /* What it does: Forces table cells into full-width rows. */
      .stack-column,
      .stack-column-center {
        display: block !important;
        width: 100% !important;
        max-width: 100% !important;
        direction: ltr !important;
      }

      /* And center justify these ones. */
      .stack-column-center {
        text-align: center !important;
      }

      /* What it does: Generic utility class for centering. Useful for images, buttons, and nested tables. */
      .center-on-narrow {
        text-align: center !important;
        display: block !important;
        margin-left: auto !important;
        margin-right: auto !important;
        float: none !important;
      }

      table.center-on-narrow {
        display: inline-block !important;
      }

      /* What it does: Adjust typography on small screens to improve readability */
      .email-container p {
        font-size: 17px !important;
        line-height: 22px !important;
      }
    }
  </style>

  <!-- What it does: Makes background images in 72ppi Outlook render at correct size. -->
  <!--[if gte mso 9]>
    <xml>
        <o:OfficeDocumentSettings>
            <o:AllowPNG/>
            <o:PixelsPerInch>96</o:PixelsPerInch>
        </o:OfficeDocumentSettings>
    </xml>
    <![endif]-->

</head>

<body width="100%" bgcolor="#F3F3F3" style="margin: 0; mso-line-height-rule: exactly;">
  <center style="width: 100%; background: #F3F3F3; text-align: left;">

    <!-- Visually Hidden Preheader Text : BEGIN -->
    <div
      style="display:none;font-size:1px;line-height:1px;max-height:0px;max-width:0px;opacity:0;overflow:hidden;mso-hide:all;">
      Alerts - WareIQ
    </div>
    <!-- Visually Hidden Preheader Text : END -->

    <!--
            Set the email width. Defined in two places:
            1. max-width for all clients except Desktop Windows Outlook, allowing the email to squish on narrow but never go wider than 680px.
            2. MSO tags for Desktop Windows Outlook enforce a 680px width.
            Note: The Fluid and Responsive templates have a different width (600px). The hybrid grid is more "fragile", and I've found that 680px is a good width. Change with caution.
        -->
    <div style="max-width: 680px; margin: auto;" class="email-container">
      <!--[if mso]>
            <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="680" align="center">
            <tr>
            <td>
            <![endif]-->

      <!-- Email Body : BEGIN -->
      <table role="presentation" cellspacing="0" border="0" cellpadding="0" align="center" width="100%"
        style="max-width: 680px;" class="email-container">
        <!-- HERO : BEGIN -->
        <tr>
          <td style="padding: 40px 0 20px;"><img
              src="__CLIENT_LOGO__" alt="__CLIENT_NAME__" width="200" height="50"/></td>
        </tr>
        <tr>
          <!-- Bulletproof Background Images c/o https://backgrounds.cm -->
          <td align="center" valign="top" style="text-align: center;" bgcolor="#ffffff">
            <div style="border-radius: 4px;margin-bottom: 30px;">
              <!--[if mso]>
                      <table role="presentation" border="0" cellspacing="0" cellpadding="0" align="center" width="680">
                      <tr>
                      <td align="center" valign="middle" width="680">
                      <![endif]-->
              <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%"
                style="max-width:680px; margin: auto;">
                <tr>
                  <td align="center" valign="middle" style="padding: 20px 20px;">
                    <div style="border-radius: 4px;overflow: hidden;">
                      <table style="width:640px;" role="presentation" border="0" cellpadding="0" cellspacing="0"
                        align="left">
                        <tr>
                          <td valign="middle" style="text-align: left; padding: 10px 20px;" bgcolor="__BACKGROUND_COLOR__">
                            <!--[if gte mso 9]>
                                    <v:rect xmlns:v="urn:schemas-microsoft-com:vml" fill="true" stroke="false" style="width:680px;height: 100px;background-position: center center !important;">
                                    <v:fill type="tile" color="__BACKGROUND_COLOR__"/>
                                    <v:textbox inset="0,0,0,0">
                                    <![endif]-->
                            <table style="width: 600px;" role="presentation" border="0" cellpadding="0" cellspacing="0"
                              align="left">
                              <tr>
                                <td valign="middle" style="text-align: left; padding: 0;" height="100" stye="height:100px">
                                  <h1 style="margin: 10px 0 0 0;font-size: 22px; line-height: 22px; color: #000000;font-weight:normal;letter-spacing: 0.5px;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">__EMAIL_TITLE__</h1>
                                </td>
                              </tr>
                            </table>
                            <!--[if gte mso 9]>
                                    </v:textbox>
                                    </v:rect>
                                    <![endif]-->
                          </td>
                        </tr>
                      </table>
                    </div>
                  </td>
                </tr>
              </table>
              <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%"
                style="max-width:680px; margin: auto;">
                <tr>
                  <td align="left" valign="middle" style="padding: 0 20px 5px 20px;">
                    <h4
                      style="margin: 0;font-size: 16px; line-height: 32px; color: #333333; font-weight: 500;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">
                      Hi __CUSTOMER_NAME__,</h4>
                  </td>
                </tr>
              </table>
              <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%"
                style="max-width:680px; margin: auto;">
                <tr>
                  <td align="left" valign="middle" style="padding: 0 20px 5px 20px;">
                    <h6
                      style="margin: 0;font-size: 15px; line-height: 26px; color: #505050; font-weight: normal;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">
                      Your order __ORDER_ID__ is shipped via __COURIER_NAME__ and is expected to reach you by __EDD__. Here are your tracking details:
                    </h6>
                  </td>
                </tr>
              </table>
              <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%"
                style="max-width:680px; margin: auto;">
                <tr>
                  <td align="left" valign="middle" style="padding: 0 20px 5px 20px;">
                    <h4
                      style="margin: 0;font-size: 14px; line-height: 32px; color: #505050; font-weight: normal;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">
                      <b>AWB Number: </b>&nbsp;&nbsp;__AWB_NUMBER__<br /></h4>
                  </td>
                    <td align="right" valign="middle" style="padding-right:100px;">
                    <h4
                      style="margin: 0;font-size: 14px; line-height: 32px; color: #505050; font-weight: normal;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">
                      <b>Courier: </b>&nbsp;&nbsp;__COURIER_NAME__</h4>
                  </td>
                </tr>
              </table>
              <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%"
                style="max-width:680px; margin: auto;">
                <tr>
                  <td align="left" valign="middle" style="padding: 5px 20px 30px 20px;">
                    <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%">
                      <tr>
                        <td width='175' align="center">
                          <div>
                            <!--[if mso]>
                                      <v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word" href="http://webapp.wareiq.com/tracking/3991610067804" style="height:38px;v-text-anchor:middle;width:150px;" arcsize="5%" strokecolor="__BACKGROUND_COLOR__" fillcolor="__BACKGROUND_COLOR__">
                                        <w:anchorlock/>
                                        <center style="color:#ffffff;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;font-size:14px;">Track Order</center>
                                      </v:roundrect>
                                    <![endif]-->
                            <a href="__TRACKING_LINK__"
                              style="background-color:__BACKGROUND_COLOR__;border:1px solid __BACKGROUND_COLOR__;border-radius:3px;color:#000000;display:inline-block;font-size:14px;line-height:36px;text-align:center;text-decoration:none;width:150px;-webkit-text-size-adjust:none;mso-hide:all;letter-spacing: 1px;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">Track Order</a>
                          </div>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>
                <table role="presentation" border="0" cellpadding="0" cellspacing="0" align="center" width="100%"
                style="max-width:680px; margin: auto;">
                <tr>
                        <td style="padding-top: 20px;border-top: 1px solid #eee;vertical-align: top;">
                          <h6
                            style="margin: 10;font-size: 14px; line-height: 22px; color: #505050; font-weight: normal;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">
                            Thank you for shopping with us,<br />
                            __CLIENT_NAME__<br />
                          </h6>
                          <p
                            style="margin: 15px 0 0 0;font-size: 12px; line-height: 26px; color: #7d7d7d; font-weight: normal;font-family: 'Poppins', sans-serif, Helvetica, Arial !important;">
                            &copy;&nbsp;2020-21 WareIQ. All Rights Reserved</p>
                        </td>
                </tr>
              </table>
              <!--[if mso]>
                      </td>
                      </tr>
                      </table>
                      <![endif]-->
            </div>
          </td>
        </tr>
        <!-- HERO : END -->
        <!-- FOOTER : BEGIN -->
        <tr>
          <td bgcolor="#ffffff">
            <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%"></table>
          </td>
        </tr>
        <!-- FOOTER : END -->

      </table>
      <!-- Email Body : END -->

      <!--[if mso]>
            </td>
            </tr>
            </table>
            <![endif]-->
    </div>

  </center>
</body>

</html>
"""