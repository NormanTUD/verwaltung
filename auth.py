from functools import wraps
from db import *
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user

def is_admin_user(session=None) -> bool:
    if session is None:
        session = Session()

    if not current_user.is_authenticated:
        session.close()
        return False

    try:
        user = session.query(User).options(joinedload(User.roles)).filter_by(id=current_user.id).one_or_none()
        if user is None:
            print(f"is_admin_user: user {current_user.id} not found")
            session.close()
            return False

        roles = [role.name for role in user.roles]
        session.close()
        return 'admin' in roles
    except Exception as e:
        print(f"is_admin_user: error: {e}")
        session.close()
        return False

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            print("admin_required: User is not authenticated")
            return render_template("admin_required.html"), 403

        session = Session()
        try:
            if not is_admin_user(session):
                print("admin_required: User is not admin")
                return render_template("admin_required.html"), 403
        except Exception as e:
            print(f"admin_required: got an error: {e}")
            return render_template("admin_required.html"), 403
        finally:
            session.close()

        return f(*args, **kwargs)
    return decorated_function
