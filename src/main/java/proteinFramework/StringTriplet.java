package proteinFramework;

public class StringTriplet {
	protected final String _firstStr;
	protected final String _secondStr;
	protected final String _thirdStr;

	public StringTriplet(String _first, String _second, String _third) {
		_firstStr = _first;
		_secondStr = _second;
		_thirdStr = _third;
	}

	public String getFirstString() {
		return _firstStr;
	}

	public String getSecondString() {
		return _secondStr;
	}
	
	public String getThirdString() {
		return _thirdStr;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((_firstStr == null) ? 0 : _firstStr.hashCode());
		result = prime * result
				+ ((_secondStr == null) ? 0 : _secondStr.hashCode());
		result = prime * result
				+ ((_thirdStr == null) ? 0 : _thirdStr.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof StringTriplet)) {
			return false;
		}
		StringTriplet other = (StringTriplet) obj;
		if (_firstStr == null) {
			if (other._firstStr != null) {
				return false;
			}
		} else if (!_firstStr.equals(other._firstStr)) {
			return false;
		}
		if (_secondStr == null) {
			if (other._secondStr != null) {
				return false;
			}
		} else if (!_secondStr.equals(other._secondStr)) {
			return false;
		}
		if (_thirdStr == null) {
			if (other._thirdStr != null) {
				return false;
			}
		} else if (!_thirdStr.equals(other._thirdStr)) {
			return false;
		}
		return true;
	}
}
