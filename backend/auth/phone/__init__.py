"""Phone-number sign-in with text-message codes (docs/auth_sms/DESIGN.md).

Everything here is inert until ``AUTH_PHONE_LOGIN_ENABLED`` is on. Modules log only event names,
short reason codes and hashes cut to 8 characters; never a phone number, a code, a message text
or vendor response text (DESIGN.md section 17).
"""
