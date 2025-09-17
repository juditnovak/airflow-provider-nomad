# This file is part of apache-airflow-providers-nomad which is
# released under Apache License 2.0. See file LICENSE or go to
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# for full license details.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.exceptions import AirflowException


class NomadProviderException(AirflowException):
    pass


class NomadValidationError(NomadProviderException):
    """Used when can't parse Nomad job input."""


class NomadOperatorError(NomadProviderException):
    """Errors for NomadJobOperator"""


class NomadJobOperatorError(NomadOperatorError):
    """Errors for NomadJobOperator"""


class NomadTaskOperatorError(NomadOperatorError):
    """Errors for NomadJobOperator"""
